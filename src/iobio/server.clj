(ns iobio.server
  "Moving toward clj iobio with full tree graphs, function nodes, superior
   error handling, logging, caching, etc.
  "

  {:author "Jon Anthony"}

  (:require
   [clojure.string     :as str]
   [clojure.set        :as set]
   [clojure.data.json  :as json]
   [ring.middleware.defaults]
   [ring.util.codec :as ruc]

   [compojure.core     :as comp
    :refer (defroutes GET POST)]

   [compojure.route    :as route]
   [hiccup.core        :as hiccup]

   [clojure.core.async :as async
    :refer (<! <!! >! >!! put! chan go go-loop alt! timeout alt! alts!!)]

   [taoensso.timbre    :as timbre
    :refer (tracef debugf infof warnf errorf)]
   [taoensso.sente     :as sente]

   [org.httpkit.server :as http-kit]
   [taoensso.sente.server-adapters.http-kit :refer (sente-web-server-adapter)]

   ;; Watch tool config directory to automatically configure new tools
   [clojure-watch.core :refer [start-watch]]

   ;; BinaryJS msg protocol
   [aerial.msgpacket.binaryjs :as bjs :refer [newBinaryJsMsg]]

   [aerial.fs :as fs]
   [aerial.utils.coll :as coll :refer [in takev-until dropv-until ensure-vec]]

   ;; For pgm graph data shape validation
   [schema.core :as sch]

   ;; Program graph construction, execution, delivery
   [iobio.pgmgraph :as pg]
   ))


;;; Logging config
;;; (sente/set-logging-level! :trace) ; Uncomment for more logging

;;; http-kit
(defn start-web-server!* [ring-handler port]
  (infof "Starting http-kit on port %s..." port)
  (let [http-kit-stop-fn (http-kit/run-server ring-handler {:port port})]
    {:server  nil ; http-kit doesn't expose this
     :port    (:local-port (meta http-kit-stop-fn))
     :stop-fn (fn [] (http-kit-stop-fn :timeout 100))}))


(defn login!
  "Here's where you'll add your server-side login/auth procedure (Friend, etc.).
  In our simplified example we'll just always successfully authenticate the user
  with whatever user-id they provided in the auth request."
  [ring-request]
  (let [{:keys [session params]} ring-request
        {:keys [user-id]} params]
    (debugf "Login request: %s" params)
    {:status 200 :session (assoc session :uid user-id)}))

(defn landing-pg-handler [req]
  (hiccup/html
    [:h1 "BinaryJS Test example"]
    [:hr]
    [:p [:strong "Not Intended to be used Manually..."]]
    ))



;;; ------------------------------------------------------------------------;;;
;;;         Configure msgprotocol, Sente and Webserver Control              ;;;
;;; ------------------------------------------------------------------------;;;

(declare
 msgpacket
 ring-ajax-post
 ring-websocket
 recvchan
 chsk-send!
 connected-uids
 my-routes
 my-ring-handler
 )

(defn config-sente []
  (let [mp (newBinaryJsMsg) ; BinaryJS msg protocol
        {:keys [ch-recv send-fn ajax-post-fn ajax-get-or-ws-handshake-fn
                connected-uids]} (sente/make-channel-socket!
                                  sente-web-server-adapter
                                  {:msgpacket mp :recv-buf-or-n 1000})]

    (alter-var-root #'msgpacket (constantly mp))
    (alter-var-root #'ring-ajax-post (constantly ajax-post-fn))
    (alter-var-root #'ring-websocket (constantly ajax-get-or-ws-handshake-fn))
    ;; ChannelSocket's receive channel
    (alter-var-root #'recvchan (constantly ch-recv))
    ;; ChannelSocket's DEFAULT send API fn
    (alter-var-root #'chsk-send! (constantly send-fn))
    ;; Watchable, read-only atom
    (alter-var-root #'connected-uids (constantly connected-uids))

    ;; Setup routes
    (alter-var-root
     #'my-routes
     (constantly
      (comp/routes
       (GET  "/"      req (landing-pg-handler req))
       ;;
       (GET  "/chsk"  req (ring-websocket req))
       (POST "/chsk"  req (ring-ajax-post req))
       ;; Static files, notably public/main.js (our cljs target)
       (route/resources "/" #_{:root "bam.iobio.io"})
       (route/not-found "<h1>Page not found</h1>"))))

    ;; Setup ring middleware handlers
    (alter-var-root
     #'my-ring-handler
     (constantly
      (let [ring-defaults-config
            (assoc-in
             ring.middleware.defaults/site-defaults [:security :anti-forgery]
             {:read-token (fn [req] (-> req :params :csrf-token))})]

        ;; NB: Sente requires the Ring `wrap-params` + `wrap-keyword-params`
        ;; middleware to work. These are included with
        ;; `ring.middleware.defaults/wrap-defaults` - but you'll need to ensure
        ;; that they're included yourself if you're not using `wrap-defaults`.
        (ring.middleware.defaults/wrap-defaults
         my-routes
         ring-defaults-config))))))

;;; Define our routes and handlers
#_(defroutes my-routes
    (GET  "/"      req (landing-pg-handler req))
    ;;
    (GET  "/chsk"  req (ring-websocket req))
    (POST "/chsk"  req (ring-ajax-post req))
    ;;
    (route/resources "/") ; Static files, notably public/main.js
    (route/not-found "<h1>Page not found</h1>"))


;;; ------------------------------------------------------------------------;;;
;;;                Control  starting and stoping Webserver                  ;;;
;;; ------------------------------------------------------------------------;;;

(declare
 start-tool-watcher
 stop-tool-watcher
 event-msg-handler*
 reset-backstreams)

(defonce web-server_ (atom nil)) ; {:server _ :port _ :stop-fn (fn [])}

(defn stop-web-server! []
  (when-let [m @web-server_]
    ((:stop-fn m))))

(defn start-web-server! [& [port]]
  (stop-web-server!)
  (let [{:keys [stop-fn port] :as server-map}
        (start-web-server!*
         (var my-ring-handler)
         (or port 0)) ; 0 => auto (any available) port
        uri (format "http://localhost:%s/" port)]
    (infof "Web server is running at `%s`" uri)
    #_(try
      (.browse (java.awt.Desktop/getDesktop)
               (java.net.URI. uri))
      (catch java.awt.HeadlessException _))
    (reset! web-server_ server-map)))


(defonce router_ (atom nil))

(defn  stop-router! []
  (when-let [stop-f @router_] (stop-f)))

(defn start-router! []
  (stop-router!)
  (reset! router_
          (sente/start-chsk-router! recvchan msgpacket event-msg-handler*)))


(defn start! [& [port]]
  (config-sente)
  (timbre/set-level! :info) ; :debug
  (reset-backstreams)
  (start-tool-watcher)
  (start-router!)
  (if port
    (start-web-server! port)
    (start-web-server!)))

(defn stop! []
  (stop-tool-watcher)
  (stop-web-server!)
  (stop-router!))


;;; ------------------------------------------------------------------------;;;
;;;                    Tool Config and  Watcher                             ;;;
;;; ------------------------------------------------------------------------;;;

(def tool-configs (atom nil))

(defn read-tool-config [f]
  (if (= "clj" (fs/ftype f))
    (-> f slurp read-string)
    (-> f slurp (json/read-str :key-fn keyword))))

(defn read-tool-configs
  []
  (let [wd (fs/pwd)
        srvdir (fs/join wd "services")
        _ (assert (fs/directory? srvdir)
                  (format "Services dir %s missing!!" srvdir))
        configs (fs/re-directory-files srvdir #"\.(clj|js)")
        toolinfo (reduce
                  (fn[M f]
                    (assoc M (-> f fs/basename (str/split #"\.") first)
                           (read-tool-config f)))
                  {} configs)]
    toolinfo))


(def _watcher (atom nil))

(defmulti
  ^{:doc "Dispatch dynamic tool config based on service file event"
    :arglists '([event filename])}
  config-tool
  (fn [event filename] event))


(defmethod config-tool :create
  [_ f]
  (swap! tool-configs
         assoc (-> f fs/basename (str/split #"\.") first)
         (read-tool-config f)))

(defmethod config-tool :modify
  [_ f]
  (swap! tool-configs
         assoc (-> f fs/basename (str/split #"\.") first)
         (read-tool-config f)))

(defmethod config-tool :delete
  [_ f]
  (swap! tool-configs
         dissoc  (-> f fs/basename (str/split #"\.") first)))

(defn stop-tool-watcher []
  (when-let [stopw @_watcher] (stopw)))

(defn start-tool-watcher []
  (stop-tool-watcher)
  (reset! _watcher
          (start-watch
           [{:path (fs/join (fs/pwd) "services")
             :event-types [:create :modify :delete]
             :bootstrap (fn[_](swap! tool-configs (fn[_] (read-tool-configs))))
             :callback config-tool
             :options {:recursive true}}])))


;;; ------------------------------------------------------------------------;;;
;;;                         Message Handlers...                             ;;;
;;; ------------------------------------------------------------------------;;;

(defmulti event-msg-handler :id) ; Dispatch on event-id

;; Wrap for logging, catching, etc.:
(defn event-msg-handler*
  [{:as ev-msg :keys [id msg client msgmap]}]
  (tracef "Event: %s, %s" id msg)
  (event-msg-handler ev-msg))


(defmethod event-msg-handler :default ; Fallback
  [{:as ev-msg :keys [id msg client msgmap]}]
  (let [ring-req (msgmap :ring-req)
        session (:session ring-req)
        uid     (:uid     session)]
    (debugf "Unhandled event: %s, %s" msg (:websocket? ring-req))))


;;; ---------- Handle Connection Open and Close ---------- ;;;

(def backstream-clients (atom {}))

(defn reset-backstreams []
  (reset! backstream-clients {}))

(defmethod event-msg-handler :connection ; new connection
  [{:as ev-msg :keys [id msg client msgmap]}]
  (let [ring-req (msgmap :ring-req)
        url (ring-req :url)]
    (tracef "CONNECTION!: %s, %s" msg (:websocket? ring-req))))

(defmethod event-msg-handler :close ; connection/client closed
  [{:as ev-msg :keys [id msg client msgmap]}]
  (debugf "Remove closed client: %s" client)
  (when client
    (swap! backstream-clients dissoc (client :connectionID))))


;;; ---------- Handle Stream Data: Run and Proxy Commands ---------- ;;;

(defn parse-url [url]
  (if (not url)
    {:query {}}
    (let [protocol (-> url (str/split #"://") first)
          [host url-params] (-> url (str/split #"\?"))
          isproxy? (= host "http://client")
          result {:protocol protocol :host host
                  :url-params url-params :isprox isproxy?}]
      (if (not url-params)
        result
        (let [pairs (->> (str/split url-params #"&")
                         (filter #(not= % ""))
                         (map #(str/split % #"="))
                         (map #(let [[x y] %]
                                 [(keyword x) (ruc/url-decode y)]))
                         (into {}))]
          (assoc result :query pairs))))))

(defn <cmd
  "Split string cmd on spaces unless spaces are in double
   quotes (\"). However remove any leading/ending double quotes from
   any resulting element.
  "
  [cmd]
  (when cmd
    (as-> cmd x (str/replace x #"\".+\"" #(str/replace % #" " "^"))
          (str/split x #" +")
          (mapv #(-> % (str/replace #"\^" " ")
                       (str/replace #"(^\"|\"$)" "")) x))))

(defn service? [x]
  (re-find #"^(http|ws)%3A%2F%2F.+\.iobio" x))

(defn get-tool-name [url & [tool]]
  (assert (or url tool) "Run Params requires url or tool field")
  (if tool tool
      (->> url (re-find #"^(ws|http)://.+\.iobio") first
           (#(str/split % #"(//|\.)")) second)))

(defn adjust-args [args toolname path inputs]
  (cond (and (= toolname "samtools") (seq inputs))
        (apply vector (first args) "-" (rest args))

        :else args))


;;; --------------------------------------------------------------------------
;;; Node pgm graph - named service defines sub program graph


(defn config-inputs-rolled [lingraph edges ins args nodes]
  #_(prn nodes)
  (reduce
   (fn[nodes n]
     (let [invec (ins n)
           node (nodes n)
           inputs (mapv #(nodes %) invec)
           numin (count inputs)
           node (cond (= 0 (count inputs))
                      node ; Root node

                      (every? #(= 1 %) (->> n edges (map #(count (ins %)))))
                      (assoc node :inputs (mapv #(nodes %) invec))

                      :else
                      (assoc node
                        :inputs []
                        :pipe (pg/get-pipe (node :id))
                        :args (replace
                               (pg/replacement-map
                                #(vector %1 ((nodes %2) :url)) "%" invec)
                               (node :args))))
           node (pg/add-input-pipes node invec nodes)
           node (assoc node
                  :args (replace
                         (pg/replacement-map
                          #(vector %1 %2) "#" args)
                         (node :args)))]
       (assoc nodes n node)))
   nodes lingraph))


(defn unroll-node [N E node]
  (cond
   (vector? node)
   (reduce (fn[V x]
             (if (:tool x)
               (let [i (unroll-node N E (x :inputs))
                     iids (mapv :id (x :inputs))
                     x (assoc x :inputs iids)]
                 (swap! N assoc (x :id) x)
                 (swap! E into (map #(vector %1 (conj (get E %1 []) %2))
                                    iids (repeat (:id x))))
                 (conj V (conj i (dissoc x :inputs))))
               (do
                 (swap! N assoc (x :id) x)
                 [x])))
           [] node)
   node node
   :else []))

(defn unroll-pipeline [pipeline]
  (if (not= (first pipeline) :rolled)
    pipeline
    ;; Else, legacy...
    (let [N (atom {})
          E (atom {})]
      (unroll-node N E (coll/dropv 1 pipeline))
      [@N @E])))

;;; --------------------------------------------------------------------------


(defn get-toolinfo [toolname]
  (assert (@tool-configs toolname)
          (format "No such tool %s" toolname))
  (let [{:keys [graph path options inputOption args]
         :or {inputOption "" options "" args ""}} (@tool-configs toolname)]
    [graph path options inputOption args]))


(defn config-stream-pipe-inputs [services]
  (reduce
   (fn[V url] #_(debugf "*** V %s, URL %s" V url)
     (let [toolname (get-tool-name url)
           [graph path options inopt defargs] (get-toolinfo toolname)

           url-info (parse-url url)
           params (reduce (fn[M [k v]] (assoc M k v)) {} (url-info :query))

           raw-args (or (<cmd (params :cmd)) [])
           [nxtsrvs args] (reduce (fn[[M A] a]
                                    (if (service? a)
                                      [(conj M (ruc/url-decode a)) A]
                                      [M (conj A (ruc/url-decode a))]))
                                  [[] []] raw-args)
           args (->> args (filterv #(not= % inopt))
                     (concat options #_defargs) vec)
           args (mapv (fn[a] (if (= (.charAt a 0) \')
                               (.substring a 1 (dec (.length a))) a)) args)

           {:keys [inputs args]}
           (group-by (fn[x]
                       (cond
                        (re-find #"^http://client" x) :inputs

                        (#{"tabix"} toolname) :args

                        (re-find #"^http://" x)
                        (if graph :inputs
                            (if (not= x (last args)) :args :inputs))

                        :else :args))
                     args)

           args (adjust-args args toolname path inputs)
           inputs (mapv (fn[url]
                          (let [info (parse-url url)
                                isprox (info :isprox)
                                urlmap {:id (gensym "http") :url url}
                                qid (when isprox (get-in info [:query :id]))
                                proxclient (when qid (@backstream-clients qid))]
                            (if isprox
                              (assoc urlmap
                                :bsc proxclient :qid qid
                                :msgpacket msgpacket)
                              urlmap)))
                        inputs)

           inputs (vec (into inputs (config-stream-pipe-inputs nxtsrvs)))]

       (conj V (cond
                (map? graph)
                (let [data (pg/node-graph graph inputs args)]
                  ((apply config-inputs-rolled data) (-> data first last)))

                :else
                {:tool path :path path :id (gensym toolname)
                 :inputs inputs :args args :inpipes [] :outpipes []}))))
   [] services))


(defn get-services [params]
  (or (params :url)
      (params :graph)))

(defn get-stream-in [edges]
  (let [ek (-> edges keys set)
        ik (-> edges pg/edges->ins keys set)]
    (first (set/difference ik ek))))

(defn config-pipe-def [stream params]
  (let [services (get-services params)
        strmid (gensym (str "strm-" (stream :id) "-"))
        strm-node {:tool "stream" :name "webstrm" :type "stream"
                   :stream stream :id strmid :path "strm"}]
    (if (map? services)
      (let [{:keys [nodes edges]} services
            si (get-stream-in edges)
            graph {:nodes (assoc nodes (keyword strmid) strm-node)
                   :edges (assoc edges si [(keyword strmid)])}]
        (-> graph
            (pg/config-pgm-graph-nodes get-toolinfo
                                       backstream-clients msgpacket)
            pg/config-pgm-graph))
      [:rolled
       (assoc strm-node
         :inputs (config-stream-pipe-inputs (coll/ensure-vec services)))])))

(def dfg-dbg (atom nil))

(defn run-command [stream params]
  (debugf "!!! RequestParams: %s" params)
  (tracef ">>> Stream = %s" stream)
  (let [cmdpath (-> (fs/pwd) (fs/join "bin"))
        _ (assert (fs/directory? cmdpath)
                  (str "iobio home directory '" (fs/pwd)
                       "' requires a 'bin' directory"))
        pipeline-config (config-pipe-def stream params)
        _ (debugf "PIPELINE-CONFIG: %s" pipeline-config)
        node-futures (->> pipeline-config
                          unroll-pipeline
                          pg/make-flow-graph
                          pg/run-flow-program)]
    (swap! dfg-dbg (fn[_] node-futures))))



(defmethod event-msg-handler :stream
  [{:as ev-msg :keys [id msg client msgmap]}]
  (let [clientid (client :id)
        [_ [stream options]] msg
        {:keys [event connectionID params]} options
        op event]
    (tracef "*** Params: Op=%s, ConnID=%s, Params=%s" op connectionID params)
    (cond
     (= op "setID") ; Obsolete, but neededd for backward compatibility...
     (let [client (assoc client :connectionID connectionID)]
       (swap! (msgmap :clients) assoc clientid client)
       (swap! backstream-clients assoc connectionID client))

     (= op "run")
     (let [params (assoc params
                    :protocol (get params :protocol "http")
                    :returnEvent (get params :returneEvent "results")
                    :encoding (if (params :binary) ; backwards compatibility
                                "binary"
                                (or (params :encoding) "utf8")))
           setfields (stream :setfields)
           ;; Set params in stream, in client stream map - Then refetch
           stream ((setfields (stream :id) :params params) (stream :id))]
       (run-command stream params)))))
