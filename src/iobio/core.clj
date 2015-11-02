(ns iobio.core
  "A robust, flexible clj iobio with full tree graphs, function nodes,
   superior error handling, logging, caching, etc.
  "
  {:author "Jon Anthony"}

  (:gen-class)

  (:require [clojure.string :as cstr]
            [clojure.java.io :as io]

            [cpath-clj.core :as cp] ; Extract Jar resources
            [clj-dns.core :as rdns] ; reverse dns

            [aerial.fs :as fs]
            [aerial.utils.misc :refer [getenv]]
            [aerial.utils.coll :as coll]

            [iobio.server :as svr])

  (:import [java.io File]
           [java.net NetworkInterface Inet4Address Inet6Address]))


(defn- digits? [s] (let [x (re-find #"[0-9]+" s)] (and x (= x s))))

(defn host-ipaddress []
  (->> (NetworkInterface/getNetworkInterfaces)
       enumeration-seq
       (filter #(and (.isUp %) (not (.isVirtual %)) (not (.isLoopback %))))
       (map #(vector (.getName %) %))
       (mapv (fn[[n intf]]
               [(keyword n)
                (->> intf .getInetAddresses enumeration-seq
                     (group-by #(cond (instance? Inet4Address %) :inet4
                                      (instance? Inet6Address %) :inet6
                                      :else :unknown))
                     (reduce (fn[M [k ifv]]
                               (assoc M k (mapv #(.getHostAddress %) ifv)))
                             {})
                     )]))
       (into {})))

(defn host-dns-name []
  (-> (host-ipaddress) :eth0 :inet4 first rdns/hostname))



(defn- get-install-dir
  "Query user for location that will be iobio installation and home
   directory
  "
  []
  (print "Enter installation directory [~/.iobio]: ")
  (flush)
  (let [input (read-line)
        input (fs/fullpath (if (= input "") "~.iobio" input))]
    (println "Selected installation location:" input)
    input))

(defn- install-host-dns-name [bamiobio]
  (let [rdr (io/reader bamiobio)
        lines (line-seq rdr)
        prefix (coll/takev-until
                #(re-find #"^ *this.clj_iobio_host" %) lines)
        [hline & suffix] (coll/dropv-until
                          #(re-find #"^ *this.clj_iobio_host" %) lines)
        host-line (str "    this.clj_iobio_host = " (host-dns-name) ";")
        tmp (fs/tempfile "bam.iobio")]
    (with-open [wtr (io/writer tmp)]
      (binding [*out* wtr]
        (doseq [l (concat prefix [host-line] suffix)]
          (println l))))
    (.close rdr)
    (fs/copy tmp bamiobio)
    (fs/delete tmp) :done))

(defn install-iobio
  "Create installation directory, mark it as the home directory and
   install required resources and set the host machine dns lookup name
   in websocket address. IOBIODIR is the directory user gave on query
   for location to install.
  "
  [iobiodir]
  (let [resmap #{"bam.iobio.io" "vcf.iobio.io"}
        resdir "resources/"]
    (println "Creating installation(iobio home) directory")
    (fs/mkdirs iobiodir)
    (println "Marking installation directory as iobio home")
    (spit (fs/join iobiodir ".iobio-home-dir") "Home directory of iobio")
    (println "Installing resources...")
    (fs/mkdir (fs/join iobiodir "pipes"))
    (fs/mkdir (fs/join iobiodir "cache"))
    (doseq [res ["services" "bin" #_"bam.iobio.io" #_"vcf.iobio.io"]]
      (doseq [[path uris] (cp/resources (io/resource res))
              :let [uri (first uris)
                    relative-path (subs path 1)
                    output-dir (->> relative-path
                                    fs/dirname
                                    (fs/join
                                     iobiodir
                                     (if (resmap res) (str resdir res) res))
                                    fs/fullpath)
                    output-file (->> relative-path
                                     (fs/join
                                      iobiodir
                                      (if (resmap res) (str resdir res) res))
                                     fs/fullpath io/file)]]
        (when (not (re-find #"^pack/" path)) ; bug in cp/resources regexer
          (println :PATH path)
          (println :RELATIVE-PATH relative-path)
          (when (not (fs/exists? output-dir))
            (fs/mkdirs output-dir))
          (println uri :->> output-file)
          (with-open [in (io/input-stream uri)]
            (io/copy in output-file))
          (when (= res "bin")
            (.setExecutable output-file true false)))))
    #_(println "Setting host dns name for websockets ...")
    #_(install-host-dns-name
       (fs/join (fs/fullpath iobiodir)
                "resources/bam.iobio.io/js/bam.iobio.js/bam.iobio.js"))
    #_(println "Host name used: " (host-dns-name))
    (println "\n\n*** Installation complete")))


(defn- find-set-home-dir
  "Tries to find the iobio home directory and, if not current working
   directory sets working directory to home directory.
  "
  []
  (let [iobioev "IOBIO_HOME"
        evdir (->  iobioev getenv fs/fullpath)
        curdir (fs/pwd)
        stdhm (fs/fullpath "~/.iobio")]
    (cond
     (and (getenv iobioev) (not= evdir curdir)
          (->> ".iobio-home-dir" (fs/join evdir) fs/exists?))
     (do (fs/cd evdir) evdir)

     (->> ".iobio-home-dir" (fs/join curdir) fs/exists?) curdir

     (and (fs/exists? stdhm) (not= stdhm curdir)
          (->> ".iobio-home-dir" (fs/join stdhm) fs/exists?))
     (do (fs/cd stdhm) stdhm)

     :else nil)))


(defn- ensure-correct-port-in-bamiobio
  "Change clj-iobio-port in bam.iobio.io if it is not the same as
   port. This ensures user can start server on different ports w/o
   needing to edit JS files. Only changes/writes new bam.iobio.js if
   the clj-iobio port in the current file is not the same as port.
  "
  [port]
  (let [bamiobio (fs/normpath
                  "resources/bam.iobio.io/js/bam.iobio.js/bam.iobio.js")
        rdr (io/reader bamiobio)
        lines (line-seq rdr)
        prefix (coll/takev-until
                #(re-find #"^ *this.clj_iobio_port" %) lines)
        [pline & suffix] (coll/dropv-until
                          #(re-find #"^ *this.clj_iobio_port" %) lines)
        curport (-> pline (cstr/split #"=") second
                    cstr/trim (cstr/split #";") first Integer.)
        port-line (str "    this.clj_iobio_port = " port ";")]
    (if (= port curport)
      (.close rdr)
      (let [tmp (fs/tempfile "bam.iobio")]
        (with-open [wtr (io/writer tmp)]
          (binding [*out* wtr]
            (doseq [l (concat prefix [port-line] suffix)]
              (println l))))
        (.close rdr)
        (fs/copy tmp bamiobio)
        (fs/delete tmp) :fixed))))

(defn- run-server
  "Run the iobio server on port PORT. Change clj-iobio-port in
   bam.iobio.io if it is not the same as port. This ensures user can
   start server on different ports w/o needing to edit JS files.
  "
  [port]
  #_(ensure-correct-port-in-bamiobio port)
  (svr/start! port))


(defn -main
  "Self installation and web server"
  [& [port]]

  (cond
   (and port (digits? port))
   (if (find-set-home-dir)
     (run-server (Integer. port))
     (do (println "iobio server must run in home directory") (System/exit 1)))

   port (do (println "Port must be an integer, e.g., 8080") (System/exit 1))

   :else ; Install iobio
   (let [iobiodir (get-install-dir)]
     (if (fs/exists? iobiodir)
       (do (println "Selected install dir already exists!") (System/exit 1))
       (install-iobio iobiodir)))))
