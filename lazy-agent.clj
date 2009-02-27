; Author: Anand Patil
; Date: Feb 5, 2009
; Common Public License

; Good reference on scheduling: Scheduling and Automatic Parallelization.
; Chapter 1 covers scheduling in DAGs and is available free on Google Books.

(set! *warn-on-reflection* true)
(ns lazy-agent)


; ==================================================
; = Utility stuff not immediately related to cells =
; ==================================================
(defn agent? [x] (instance? clojure.lang.Agent x))
(defn id? [x] (instance? clojure.lang.IDeref x))
(defn deref-or-val [x] (if (id? x) @x x))

(defn map-now [fn coll] (doall (map fn coll)))
(defn second-arg [x y] y)
(defn set-agent! [a v] (send a second-arg v))


; ==================
; = Updating stuff =
; ==================

(defn complete-parents [val parents]
    "Takes a map of the form {parent @parent}, and a list of mutable and
    immutable parents, and returns a list of the parents' values in the
    correct order."
    (loop [parents-sofar parents val-sofar (list)]
        (if (empty? parents-sofar) val-sofar
            (let [
            parent (last parents-sofar) 
            rest-parents (butlast parents-sofar) 
            this-val (val parent)
            ]
            (if this-val
                ; If value has a key corresponding to this parent, cons the corresponding value
                (recur rest-parents (cons this-val val-sofar))
                ; Otherwise, cons the parent.
                (recur rest-parents (cons (deref-or-val parent) val-sofar)))))))

(defn swap-id-parent-value [val parent parent-val]
    "Utility function that incorporates updated parents into a cell's
    parent value ref." 
    (if (:needs-update parent-val) 
        (dissoc val parent)
        (assoc val parent parent-val)))

(defn updating-fn [x] (if (:needs-update x) (assoc x :updating true) x))
(defn force-updating-fn [x] {:needs-update true :updating true})
(defn send-force-update [p] (send p force-updating-fn))
(defn send-update [p] "Utility function that puts p into the updating state." (send p updating-fn))

(defn compute [parents agent-parent-vals update-fn] 
    "Utility function that applies a cell's updating function to its
    parents." 
    (apply update-fn (complete-parents @agent-parent-vals parents)))

(defn report-to-child [val parent parent-val id-parents parents update-fn id-parent-vals oblivious?]
    "Called by parent-watcher when a parent either updates or reverts to
    the 'need-update' state. If a parent updates and the child cell wants
    to update, computation is performed if possible. If a parent reverts
    to the need-to-update state, the child is put into the need-to-update 
    state also."
    (do (dosync (commute id-parent-vals swap-id-parent-value parent parent-val))
        (if (:updating val) 
            (if (= (count @id-parent-vals) (count id-parents))
                (compute parents id-parent-vals update-fn)
                val)
            (if (or (:needs-update val) oblivious?) 
                val 
                {:needs-update true}))))
        
(defn watcher-to-watch [fun]
    "Converts an 'old-style' watcher function to a new synchronous watch.
    The watcher is used as the key of the new watch."
    (fn [watcher reference old-val new-val]
        (if (not= old-val new-val)
            (send watcher fun reference new-val))))        
        
(defn parent-watcher [id-parents parents update-fn id-parent-vals oblivious?]
    "Watches a parent cell on behalf of one of its children. This watcher 
    has access to a ref which holds the values of all the target child's 
    updated parents. It also reports parent chages to the child."
    (fn [cell-val p p-val]
        (if (not (:updating p-val))
            (report-to-child cell-val p p-val id-parents parents update-fn id-parent-vals oblivious?)
            cell-val)))

(defn cell-watcher [cell id-parents id-parent-vals agent-parents parents update-fn]    
    "Watches a non-root cell. If it changes and requests an update,
    it computes if possible. Otherwise it sends an update request to all its
    parents."
    (let [compute-fn (fn [junk] (compute parents id-parent-vals update-fn))]
        (fn cell-watcher [key cell old-val cell-val]
            (if (not= old-val cell-val)
                (if (:updating cell-val)
                    (if (= (count @id-parent-vals) (count id-parents))
                        (send cell compute-fn)
                        (map-now send-update agent-parents)))))))

; =======================
; = Cell creation stuff =
; =======================

(defn updated? [c] (not (:needs-update @c)))
(defn cell [name update-fn parents & [oblivious?]]
    "Creates a cell (lazy auto-agent) with given update-fn and parents."
    (let [
        cell (agent {:needs-update true})
        id-parents (filter id? parents)
        agent-parents (filter agent? id-parents)
        updated-parents (filter updated? id-parents)          
        id-parent-vals (ref (zipmap updated-parents (map deref updated-parents)))
        add-parent-watcher (fn [p] (add-watch p cell (watcher-to-watch (parent-watcher id-parents parents update-fn id-parent-vals oblivious?))))
        ]
        (do
            ; Add a watcher to all the cell's parents            
            (map-now add-parent-watcher id-parents)
            (add-watch cell :key (cell-watcher cell id-parents id-parent-vals agent-parents parents update-fn))
            cell)))

(defmacro def-cell
    "Creates and inters a cell in the current namespace, bound to sym,
    with given parents and update function."
    [sym update-fn parents & [oblivious?]] 
    `(def ~sym (cell ~@(name sym) ~update-fn ~parents ~oblivious?)))


; =================================================
; = Async and synchronized multi-cell evaluations =
; =================================================

(defn update [& cells] "Asynchronously updates the cells and returns immediately."(map-now send-update cells))
(defn force-update [& cells] "Asynchronously updates the cells and returns immediately."(map-now send-force-update cells))

(defn unlatching-watcher [#^java.util.concurrent.CountDownLatch latch cell old-val new-val]
    "A watcher function that decrements a latch when a cell updates."
    (do
        (if (not= old-val new-val)
            (if (not (:updating new-val))
                (.countDown latch)))
            latch))

(defn evaluate [& cells]
    "Updates the cells, blocks until the computation is complete, returns their values."
    (let [        
          latch (java.util.concurrent.CountDownLatch. (count (filter (comp not updated?) cells)))
          watcher-adder (fn [cell] (add-watch cell latch unlatching-watcher))
          watcher-remover (fn [cell] (remove-watch cell latch))
          ]
        (do
            (map-now watcher-adder cells)            
            (apply update cells)             
            (.await latch)
            (map-now watcher-remover cells)
            (map deref cells))))