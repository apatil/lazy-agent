; Author: Anand Patil
; Date: Feb 5, 2009
; Creative Commons BY-SA, see LICENSE
; copyright 2009 Anand Patil

; Good reference on scheduling: Scheduling and Automatic Parallelization.
; Chapter 1 covers scheduling in DAGs and is available free on Google Books.

; TODO: Make a fn analogous to synchronize that adds a watcher with a specified action to cells, which waits till they compute and then dispatches the action with the cells' values.
; TODO: Abbreviations:
;- la : lazy agent
;- p : parent
;- pv : parent value / parent val
;- cv : cell value / cell val
;- cm : cell meta
;- obliv : oblivious
; TODO: Shorten code with macros.
; TODO: Propagate exceptions.

;(set! *warn-on-reflection* true)


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
(def needs-update-value (struct cell-val nil :needs-update))
(def deref-cell (comp cell-value deref))

(defstruct cell-meta :agent-parents :id-parent-vals :n-id-parents :parents :fn :oblivious? :lazy-agent)
(def cell-meta-agent-parents (accessor cell-meta :agent-parents))
(def cell-meta-id-parent-vals (accessor cell-meta :id-parent-vals))
(def cell-meta-n-id-parents (accessor cell-meta :n-id-parents))
(def cell-meta-parents (accessor cell-meta :parents))
(def cell-meta-fn (accessor cell-meta :fn))
(def cell-meta-oblivious? (accessor cell-meta :oblivious?))
(def cell-meta-lazy-agent (accessor cell-meta :lazy-agent))
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
(defn force-need-update-fn [x] (with-meta needs-update-value (meta x)))
(defn send-force-need-update [p] "Utility function that puts p into the needs-update state, even if p is oblivious." (send p force-need-update-fn))
(defn send-update [p] "Utility function that puts p into the updating state if it needs an update." (send p updating-fn))

(defn update [& cells] "Asynchronously updates the cells and returns immediately."(map-now send-update cells))
(defn force-need-update [& cells] "Asynchronously updates the cells and returns immediately."(map-now send-force-need-update cells))
        
(defn complete-parents [parent-val-map parents]
    "Takes a map of the form {parent @parent}, and a list of mutable and
    immutable parents, and returns a list of the parents' values in the
    correct order."
    (loop [parents-sofar parents val-sofar (list)]
        (if (empty? parents-sofar) val-sofar
            (let [parent (last parents-sofar) 
                rest-parents (butlast parents-sofar)]
                (if (id? parent)
                    ; If value has a key corresponding to this parent, cons the corresponding value
                    (recur rest-parents (cons (parent-val-map parent) val-sofar))
                    ; Otherwise, cons the parent.
                    (recur rest-parents (cons parent val-sofar)))))))

(defn compute-cell-value [cur-val cur-meta id-parent-vals new-status]
    "Can be sent to a cell when its id-parent-vals are complete to compute its value."
    (let [parents (cell-meta-parents cur-meta)
        update-fn (cell-meta-fn cur-meta)
        new-parents (complete-parents id-parent-vals parents)
        new-val (apply update-fn new-parents)] 
        ; Create new value, preserving metadata, and put cell in either up-to-date or oblivious state.
        (with-meta (struct cell-val new-val new-status) cur-meta)))
        
(defn swap-la-parent-value [parent-val-map parent parent-val]
    "Utility function that incorporates updated parents into a cell's
    parent value ref." 
    ; If the parent is a lazy agent, check whether it's switched into the 
    ; needs update state, otherwise record its new value.
    (if (needs-update? parent-val) 
        (dissoc parent-val-map parent)
        (assoc parent-val-map parent (cell-value parent-val))))

(defn swap-id-parent-value [parent-val-map parent parent-val]
    ; Otherwise, just record its new value.
    (assoc parent-val-map parent parent-val))

(defn reaction [val meta]
    ; If the child is not oblivious, put it in the needs-update state.
    (with-meta (if (needs-update? val) val needs-update-value) meta))

(defn report-to-child [parent-lazy-agent? oblivious?]
    "Called by parent-watcher when a parent either updates or reverts to
    the 'needs-update' state. If a parent updates and the child cell wants
    to update, computation is performed if possible. If a parent reverts
    to the needs-update state, the child is put into the needs-update 
    state also."
    (let [swap-fn (if parent-lazy-agent? swap-la-parent-value swap-id-parent-value)
            react-fn (if oblivious? with-meta reaction)
            updated-status (if oblivious? :oblivious :up-to-date)]
        (fn [cur-val parent parent-val]
            (let [cur-meta (meta cur-val)
                new-id-parent-vals (swap-fn (cell-meta-id-parent-vals cur-meta) parent parent-val)
                new-meta (assoc cur-meta :id-parent-vals new-id-parent-vals)] 
            ; If the child is updating, check whether it's ready to compute.
            (if (updating? cur-val) 
                (if (= (count new-id-parent-vals) (cell-meta-n-id-parents new-meta))
                    ; Compute if possible, otherwise do nothing.
                    (compute-cell-value cur-val new-meta new-id-parent-vals updated-status)
                    (with-meta cur-val new-meta))
                ; React to the new value.
                (react-fn cur-val new-meta))))))

(defn watcher-to-watch [fun]
    "Converts an 'old-style' watcher function to a new synchronous watch.
    The watcher is used as the key of the new watch."
    (fn [watcher reference old-val new-val]
        (if (not= old-val new-val)
            (send watcher fun reference new-val))))        

(defn parent-watcher [oblivious?]
    "Watches a parent cell on behalf of one of its children. This watcher 
    has access to a ref which holds the values of all the target child's 
    updated parents. It also reports parent chages to the child."
    (let [report (report-to-child true oblivious?)]
        (fn [cur-val p p-val]
            (if (not (updating? p-val))
                (report cur-val p p-val)
                cur-val))))

(defn cell-watch [updated-status cell old-val cell-val]
    "Watches a non-root cell. If it changes and requests an update,
    it computes if possible. Otherwise it sends an update request to all its
    parents."
    (if (not= old-val cell-val)
        (if (updating? cell-val)
            (let [cell-meta (meta cell-val)
                    id-parent-vals (cell-meta-id-parent-vals cell-meta)
                    num-id-parent-vals (count id-parent-vals)
                    num-id-parents (cell-meta-n-id-parents cell-meta)]
                ; If the cell has changed into the updating state, check whether an immediate computation is possible.
                (if (=  num-id-parent-vals num-id-parents)
                    ; Compute if possible
                    (send cell compute-cell-value cell-meta id-parent-vals updated-status)
                    ; Otherwise put all parents that need updates into the updating state.
                    (map-now send-update (cell-meta-agent-parents cell-meta)))))))

; =======================
; = Cell creation stuff =
; =======================

(defn updated? [c] (not (= (-> c deref :status) :needs-update)))
(defn extract-value [x] (let [v (:value x)] (if v v x)))
(def extract-cell-value (comp extract-value deref))
(defn cell [name update-fn parents & [oblivious?]]
    "Creates a cell (lazy auto-agent) with given update-fn and parents."
    (let [parents (vec parents)
        id-parents (set (filter id? parents))
        n-id-parents (count id-parents)
        agent-parents (set (filter agent? id-parents))
        updated-parents (filter updated? id-parents)          
        id-parent-vals (zipmap updated-parents (map extract-cell-value updated-parents))
        cell (agent (with-meta
                        needs-update-value
                        (struct cell-meta agent-parents id-parent-vals n-id-parents parents update-fn oblivious? true)))        
        add-parent-watcher (fn [p] (add-watch p cell (watcher-to-watch 
                                (if (is-lazy-agent? p) (parent-watcher oblivious?) (report-to-child false oblivious?)))))]
        (do
            ; Add a watcher to all the cell's parents
            (map-now add-parent-watcher id-parents)
            ; Add watcher to the cell that propagates update requests to parents.
            (add-watch cell (if oblivious? :oblivious :up-to-date) cell-watch)
            cell)))

(defmacro def-cell
    "Creates and inters a cell in the current namespace, bound to sym,
    with given parents and update function."
    [sym update-fn parents & [oblivious?]] 
    `(def ~sym (cell ~@(name sym) ~update-fn ~parents ~oblivious?)))

; =======================================
; = Synchronized multi-cell evaluations =
; =======================================
(defn not-waiting? [cell-val] 
    "Determines whether a cell is either up-to-date or oblivious."
    (let [status (cell-status cell-val)]
        (or 
            (= :up-to-date status) 
            (= :oblivious status))))

(defn unlatching-watcher [#^java.util.concurrent.CountDownLatch latch cell old-val new-val]
    "A watcher function that decrements a latch when a cell updates."
    (do
        (if (not= old-val new-val)
            (if (not-waiting? new-val)
                (.countDown latch)))
            latch))

(def cell-waiting? (comp not not-waiting? deref))
(defn synchronize [async-fn]
    "Takes any function that takes any number of cells as an argument,
    puts the cells through the :updating state at some point, and
    returns immediately. Returns a function that does the same thing,
    but waits for the result and returns it."
    (fn [& cells]
        (let [        
              latch (java.util.concurrent.CountDownLatch. (count (filter cell-waiting? cells)))
              watcher-adder (fn [cell] (add-watch cell latch unlatching-watcher))
              watcher-remover (fn [cell] (remove-watch cell latch))]
            (do
                (map-now watcher-adder cells)            
                (apply async-fn cells)             
                (.await latch)
                (map-now watcher-remover cells)
                (map deref-cell cells)))))
                
(def evaluate (synchronize update))
(def force-evaluate (synchronize (comp force-need-update update)))

; ============================
; = Change cell dependencies =
; ============================

(defn conditional-map-replace [old-map old-key new-key dissoc-cond assoc-cond]
    "Utility function for switching parents."
    (let [dissoc-map (if dissoc-cond (dissoc old-map old-key) old-map)
            val (deref-or-val new-key)]
        (if assoc-cond 
            (if (up-to-date? val) (assoc dissoc-map new-key (extract-value val)) dissoc-map)
            dissoc-map)))
        
(defn conditional-set-replace [old-set old-val new-val disj-cond conj-cond]
    "Utility function for switching parents."
    (let [disj-set (if disj-cond (disj old-set old-val) old-set)]
        (if conj-cond (conj disj-set new-val) disj-set)))
        
(defn conditional-counter-change [old-ctr dec? inc?]
    "Utility function for switching parents."    
    (let [dec-ctr (if dec? (- old-ctr 1) old-ctr)]
        (if inc? (+ dec-ctr 1) dec-ctr)))

(defn replace-parent-msg [cell-val old-parent new-parent]
    "This message is sent to cells by replace-parent."
    (let [old-meta (meta cell-val)
        parents (replace {old-parent new-parent} (cell-meta-parents old-meta))
        new-id? (id? new-parent)
        old-id? (id? old-parent)
        new-agent? (agent? new-parent)
        old-agent? (agent? old-parent)
        agent-parents (conditional-set-replace (cell-meta-agent-parents old-meta) old-parent new-parent old-agent? new-agent?)
        id-parent-vals (conditional-map-replace (cell-meta-id-parent-vals old-meta) old-parent new-parent old-id? new-id?)
        n-id-parents (conditional-counter-change (cell-meta-n-id-parents old-meta) old-id? new-id?)]
    (with-meta (if (= (cell-status cell-val) :oblivious) cell-val needs-update-value)
        (assoc old-meta 
            :parents parents
            :agent-parents agent-parents 
            :id-parent-vals id-parent-vals 
            :n-id-parents n-id-parents))))
            
(defn replace-parent [cell old-parent new-parent]
    "Replaces a cell's parent. Sets the cell's value to needs-update, or leaves
    it unchanged if the cell is oblivious."
    (do 
        ; Remove old watcher
        (remove-watch old-parent cell)                                                            
        ; Tell the cell to update its metadata and go into the needs-update state if not oblivious.
        (send cell replace-parent-msg old-parent new-parent)                      
        ; Add new watcher.                
        (add-watch new-parent cell 
            (watcher-to-watch 
                (let [oblivious? (-> cell deref meta cell-meta-oblivious?)]
                    (if (is-lazy-agent? new-parent) 
                    (parent-watcher oblivious?) 
                    (report-to-child false oblivious?)))))))