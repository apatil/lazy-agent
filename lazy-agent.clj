; Author: Anand Patil
; Date: Feb 5, 2009
; Creative Commons BY-SA, see LICENSE
; copyright 2009 Anand Patil

; Good reference on scheduling: Scheduling and Automatic Parallelization.
; Chapter 1 covers scheduling in DAGs and is available free on Google Books.

(set! *warn-on-reflection* true)

; ==================================================
; = Utility stuff not immediately related to cells =
; ==================================================
(ns lazy-agent)
(defn agent? [x] (instance? clojure.lang.Agent x))
(defn id? [x] (instance? clojure.lang.IDeref x))
(defn deref-or-val [x] (if (id? x) @x x))
(defn map-now [fn coll] (doall (map fn coll)))

; ============================================
; = Structmaps and accessors for cell values =
; ============================================
(defstruct cell-val :value :status)
(def cell-value (accessor cell-val :value))
(def cell-status (accessor cell-val :status))
(def deref-cell (comp cell-value deref))

(defstruct cell-meta :agent-parents :id-parent-vals :id-parents :parents :fn :oblivious? :lazy-agent)
(def cell-meta-agent-parents (accessor cell-meta :agent-parents))
(def cell-meta-id-parent-vals (accessor cell-meta :id-parent-vals))
(def cell-meta-id-parents (accessor cell-meta :id-parents))
(def cell-meta-parents (accessor cell-meta :parents))
(def cell-meta-fn (accessor cell-meta :fn))
(def cell-meta-oblivious? (accessor cell-meta :oblivious?))
(defn is-lazy-agent? [x] (-> x deref meta :lazy-agent))

(defn up-to-date? [cell] (= :up-to-date (cell-status cell)))
(defn oblivious? [cell] (= :oblivious (cell-status cell)))
(defn updating? [cell] (= :updating (cell-status cell)))
(defn needs-update? [cell] (= :needs-update (cell-status cell)))
(defn second-arg [x y] y)
(defn set-agent! [a v] (send a second-arg v))
(defn set-cell! [c v] (send c (fn [x] (struct cell-val :value v :status :up-to-date))))

; ==================
; = Updating stuff =
; ==================

(defn updating-fn [x] (if (needs-update? x) (assoc x :status :updating) x))
(def needs-update-value (struct cell-val nil :needs-update))
(defn force-need-update-fn [x] (with-meta needs-update-value (meta x)))
(defn send-force-need-update [p] "Utility function that puts p into the needs-update state, even if p is oblivious." (send p force-need-update-fn))
(defn send-update [p] "Utility function that puts p into the updating state if it needs an update." (send p updating-fn))

(defn update [& cells] "Asynchronously updates the cells and returns immediately."(map-now send-update cells))
(defn force-need-update [& cells] "Asynchronously updates the cells and returns immediately."(map-now send-force-need-update cells))

; TODO: Eliminate if in extract-id-val and if lazy-agent? in swap-id-parent-value, and make complete-parents less check-ish.

(defn extract-id-val [id-val]
    "Gets the value out of id-val, whether it's a cell value struct or just a plain object."
    (let [val (:value id-val)]
        (if val val id-val)))
        
(defn complete-parents [val parents]
    "Takes a map of the form {parent @parent}, and a list of mutable and
    immutable parents, and returns a list of the parents' values in the
    correct order."
    (loop [parents-sofar parents val-sofar (list)]
        (if (empty? parents-sofar) val-sofar
            (let [parent (last parents-sofar) 
                rest-parents (butlast parents-sofar) 
                this-val (val parent)
                ]
                (if this-val
                    ; If value has a key corresponding to this parent, cons the corresponding value
                    (recur rest-parents (cons (extract-id-val this-val) val-sofar))
                    ; Otherwise, cons the parent.
                    (recur rest-parents (cons parent val-sofar)))))))

; TODO: compute-cell-value shouldn't doubly examine the metadata when called by
; report-to-child.
(defn compute-cell-value [cur-val cur-meta id-parent-vals]
    "Can be sent to a cell when its id-parent-vals are complete to compute its value."
    (let [parents (cell-meta-parents cur-meta)
        update-fn (cell-meta-fn cur-meta)
        oblivious? (cell-meta-oblivious? cur-meta)
        new-parents (complete-parents id-parent-vals parents)
        new-val (apply update-fn new-parents)
        new-status (if oblivious? :oblivious :up-to-date)
        ] 
        ; Create new value, preserving metadata, and put cell in either up-to-date or oblivious state.
        (with-meta (struct cell-val new-val new-status) cur-meta)))
        
(defn swap-id-parent-value [val parent parent-val lazy-agent?]
    "Utility function that incorporates updated parents into a cell's
    parent value ref." 
    (if lazy-agent?
        ; If the parent is a lazy agent, check whether it's switched into the 
        ; needs update state, otherwise record its new value.
        (if (needs-update? parent-val) 
            (dissoc val parent)
            (assoc val parent parent-val))
        ; Otherwise, just record its new value.
        (assoc val parent parent-val)))

(defn report-to-child [cur-val parent parent-val & lazy-agent?]
    "Called by parent-watcher when a parent either updates or reverts to
    the 'need-update' state. If a parent updates and the child cell wants
    to update, computation is performed if possible. If a parent reverts
    to the need-to-update state, the child is put into the need-to-update 
    state also."
    (let [cur-meta (meta cur-val)
            new-id-parent-vals (swap-id-parent-value (cell-meta-id-parent-vals cur-meta) parent parent-val lazy-agent?)
            new-meta (assoc cur-meta :id-parent-vals new-id-parent-vals)
            new-val (with-meta cur-val new-meta)] 
        ; If the child is updating, check whether it's ready to compute.
        (if (updating? new-val) 
            (if (= (count new-id-parent-vals) (count (cell-meta-id-parents new-meta)))
                ; Compute if possible, otherwise do nothing.
                (compute-cell-value new-val new-meta new-id-parent-vals)
                new-val)
            ; If the child is not ready to compute, and needs an update or is oblivious, leave it alone.
            (if (or (needs-update? new-val) (oblivious? new-val)) 
                new-val
                ; Otherwise put it into the needs-update state.
                (with-meta needs-update-value new-meta)))))
;        
(defn watcher-to-watch [fun]
    "Converts an 'old-style' watcher function to a new synchronous watch.
    The watcher is used as the key of the new watch."
    (fn [watcher reference old-val new-val]
        (if (not= old-val new-val)
            (send watcher fun reference new-val))))        

(defn parent-watcher [cur-val p p-val]
    "Watches a parent cell on behalf of one of its children. This watcher 
    has access to a ref which holds the values of all the target child's 
    updated parents. It also reports parent chages to the child."
        (if (not (updating? p-val))
            (report-to-child cur-val p p-val true)
            cur-val))

(defn cell-watch [key cell old-val cell-val]
    "Watches a non-root cell. If it changes and requests an update,
    it computes if possible. Otherwise it sends an update request to all its
    parents."
    (if (not= old-val cell-val)
        (if (updating? cell-val)
            (let [cell-meta (meta cell-val)
                    id-parent-vals (cell-meta-id-parent-vals cell-meta)
                    num-id-parent-vals (count id-parent-vals)
                    num-id-parents (-> cell-meta cell-meta-id-parents count)
                    agent-parents (cell-meta-agent-parents cell-meta)]
                ; If the cell has changed into the updating state, check whether an immediate computation is possible.
                (if (=  num-id-parent-vals num-id-parents)
                    ; Compute if possible
                    (send cell compute-cell-value cell-meta id-parent-vals)
                    ; Otherwise put all parents that need updates into the updating state.
                    (map-now send-update agent-parents))))))

; =======================
; = Cell creation stuff =
; =======================

(defn updated? [c] (not (= (-> c deref :status) :needs-update)))
(defn cell [name update-fn parents & [oblivious?]]
    "Creates a cell (lazy auto-agent) with given update-fn and parents."
    (let [
        id-parents (filter id? parents)
        agent-parents (filter agent? id-parents)
        updated-parents (filter updated? id-parents)          
        id-parent-vals (zipmap updated-parents (map deref updated-parents))
        cell (agent (with-meta
                        needs-update-value
                        (struct cell-meta agent-parents id-parent-vals id-parents parents update-fn oblivious? true)))        
        add-parent-watcher (fn [p] (add-watch p cell (watcher-to-watch 
                                (if (is-lazy-agent? p) parent-watcher report-to-child))))
        ]
        (do
            ; Add a watcher to all the cell's parents
            (map-now add-parent-watcher id-parents)
            ; Add watcher to the cell that propagates update requests to parents.
            (add-watch cell :self cell-watch)
            cell)))

(defmacro def-cell
    "Creates and inters a cell in the current namespace, bound to sym,
    with given parents and update function."
    [sym update-fn parents & [oblivious?]] 
    `(def ~sym (cell ~@(name sym) ~update-fn ~parents ~oblivious?)))


; =======================================
; = Synchronized multi-cell evaluations =
; =======================================


(defn unlatching-watcher [#^java.util.concurrent.CountDownLatch latch cell old-val new-val]
    "A watcher function that decrements a latch when a cell updates."
    (do
        (if (not= old-val new-val)
            (if (up-to-date? new-val)
                (.countDown latch)))
            latch))

(defn synchronize [async-fn]
    "Takes any function that takes any number of cells as an argument,
    puts the cells through the :updating state at some point, and
    returns immediately. Returns a function that does the same thing,
    but waits for the result and returns it."
    (fn [& cells]
        (let [        
              latch (java.util.concurrent.CountDownLatch. (count (filter (comp not up-to-date? deref) cells)))
              watcher-adder (fn [cell] (add-watch cell latch unlatching-watcher))
              watcher-remover (fn [cell] (remove-watch cell latch))
              ]
            (do
                (map-now watcher-adder cells)            
                (apply async-fn cells)             
                (.await latch)
                (map-now watcher-remover cells)
                (map deref cells)))))

(def evaluate (synchronize update))