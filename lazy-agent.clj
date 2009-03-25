; Author: Anand Patil
; Date: Feb 5, 2009
; Creative Commons BY-SA, see LICENSE
; copyright 2009 Anand Patil

; Good reference on scheduling: Scheduling and Automatic Parallelization.
; Chapter 1 covers scheduling in DAGs and is available free on Google Books.

; TODO: Make a fn analogous to synchronize that adds a watcher with a specified action to cells, which waits till they compute and then dispatches the action with the cells' vals.
; TODO: Propagate exceptions.
; TODO: Transactions won't actually work for rejecting jumps. When a cell computes, have its update-fn memoize its last arguments.
; TODO: Transactions won't actually work for pipelining either. Just make this one actor-based, then make an implementation later that's truly stateless.
; Then you can pipeline all you want.


;(set! *warn-on-reflection* true)

; ==================================================
; = Utility stuff not immediately related to cells =
; ==================================================
(ns lazy-agent)

(defmacro structmap-and-accessors [sym & fields]
    "Defunes a structmap with given symbol, and automatically creates accessors for all its fields."
    (let [code-lst `(defstruct ~sym ~@fields)
            sym-dash (.concat (name sym) "-")
            accessor-names (zipmap fields (map (comp #(.concat sym-dash %1) name) fields))]
        (cons 'do (cons code-lst
            (for [field fields] (let [n (accessor-names field) s (symbol n)]
                `(def ~s (accessor ~sym ~field))))))))
                
(defn agent? [x] (instance? clojure.lang.Agent x))
(defn id? [x] (instance? clojure.lang.IDeref x))
(defn deref-or-val [x] (if (id? x) @x x))
(defn map-now [fn coll] (dorun (map fn coll)))

; ============================================
; = Structmaps and accessors for cell vals =
; ============================================
(structmap-and-accessors cell-val :val :status)
(def needs-update-val (struct cell-val nil :needs-update))
(def deref-cell (comp cell-val-val deref))

(structmap-and-accessors cell-meta :agent-parents :id-parent-vals :n-id-parents :parents :fn :oblivious? :lazy-agent)
(defn is-lazy-agent? [x] (-> x deref meta :lazy-agent))

(defn up-to-date? [cell] (= :up-to-date (cell-val-status cell)))
(defn oblivious? [cell] (= :oblivious (cell-val-status cell)))
(defn inherently-oblivious? [cell-val] (-> cell-val meta cell-meta-oblivious?))
(defn updating? [cell] (= :updating (cell-val-status cell)))
(defn needs-update? [cell] (= :needs-update (cell-val-status cell)))
(defn second-arg [x y] y)

(defn set-cell! [c v] 
    "Sets a cell's val to v, and sets its status to either :updated or :oblivious as appropriate."
    (send c 
        (fn [old-v] (let [updated-status (if (inherently-oblivious? old-v) :oblivious :up-to-date)]
            (with-meta (struct cell-val v updated-status) (meta v))))))

; ==================
; = Updating stuff =
; ==================

(defn updating-fn [x] (if (needs-update? x) (assoc x :status :updating) x))
(defn force-need-update-fn [x] (with-meta needs-update-val (meta x)))
(defn send-force-need-update [p] "Utility function that puts p into the needs-update state, even if p is oblivious." (send p force-need-update-fn))
(defn send-update [p] "Utility function that puts p into the updating state if it needs an update." (send p updating-fn))

(defn update [& cells] "Asynchronously updates the cells and returns immediately."(map-now send-update cells))
(defn force-need-update [& cells] "Asynchronously puts the cells in :needs-update and returns immediately."(map-now send-force-need-update cells))
        
(defn complete-parents [parent-val-map parents]
    "Takes a map of the form {parent @parent}, and a list of mutable and
    immutable parents, and returns a list of the parents' vals in the
    correct order."
    (loop [parents-sofar parents val-sofar (list)]
        (if (empty? parents-sofar) val-sofar
            (let [parent (last parents-sofar) 
                rest-parents (butlast parents-sofar)]
                (if (id? parent)
                    ; If val has a key corresponding to this parent, cons the corresponding val
                    (recur rest-parents (cons (parent-val-map parent) val-sofar))
                    ; Otherwise, cons the parent.
                    (recur rest-parents (cons parent val-sofar)))))))

(defn compute-cell-val [cur-val cur-meta id-parent-vals new-status]
    "Can be sent to a cell when its id-parent-vals are complete to compute its val."
    (let [parents (cell-meta-parents cur-meta)
        update-fn (cell-meta-fn cur-meta)
        new-parents (complete-parents id-parent-vals parents)
        new-val (apply update-fn new-parents)] 
        ; Create new val, preserving metadata, and put cell in either up-to-date or oblivious state.
        (with-meta (struct cell-val new-val new-status) cur-meta)))
        
(defn swap-la-parent-val [parent-val-map parent parent-val]
    "Utility function that incorporates updated parents into a cell's
    parent val ref." 
    ; If the parent is a lazy agent, check whether it's switched into the 
    ; needs update state, otherwise record its new val.
    (if (needs-update? parent-val) 
        (dissoc parent-val-map parent)
        (assoc parent-val-map parent (cell-val-val parent-val))))

(defn swap-id-parent-val [parent-val-map parent parent-val]
    ; Otherwise, just record its new val.
    (assoc parent-val-map parent parent-val))

(defn reaction [val meta]
    ; If the child is not oblivious, put it in the needs-update state.
    (with-meta (if (needs-update? val) val needs-update-val) meta))

(defn report-to-child [parent-lazy-agent? oblivious?]
    "Called by parent-watcher when a parent either updates or reverts to
    the 'needs-update' state. If a parent updates and the child cell wants
    to update, computation is performed if possible. If a parent reverts
    to the needs-update state, the child is put into the needs-update 
    state also."
    (let [swap-fn (if parent-lazy-agent? swap-la-parent-val swap-id-parent-val)
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
                    (compute-cell-val cur-val new-meta new-id-parent-vals updated-status)
                    (with-meta cur-val new-meta))
                ; React to the new val.
                (react-fn cur-val new-meta))))))

(defn watcher-to-watch [fun]
    "Converts an 'old-style' watcher function to a new synchronous watch.
    The watcher is used as the key of the new watch."
    (fn [watcher reference old-val new-val]
        (if (not= old-val new-val)
            (send watcher fun reference new-val))))        

(defn parent-watcher [oblivious?]
    "Watches a parent cell on behalf of one of its children. This watcher 
    has access to a ref which holds the vals of all the target child's 
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
                    (send cell compute-cell-val cell-meta id-parent-vals updated-status)
                    ; Otherwise put all parents that need updates into the updating state.
                    (map-now send-update (cell-meta-agent-parents cell-meta)))))))

; =======================
; = Cell creation stuff =
; =======================

(defn updated? [c] (not (= (-> c deref :status) :needs-update)))
(defn extract-val [x] (let [v (:val x)] (if v v x)))
(def extract-cell-val (comp extract-val deref))
(defn cell [name update-fn parents & [oblivious?]]
    "Creates a cell (lazy auto-agent) with given update-fn and parents."
    (let [parents (vec parents)
        id-parents (set (filter id? parents))
        n-id-parents (count id-parents)
        agent-parents (set (filter agent? id-parents))
        updated-parents (filter updated? id-parents)          
        id-parent-vals (zipmap updated-parents (map extract-cell-val updated-parents))
        cell (agent (with-meta
                        needs-update-val
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
    `(def ~sym (cell ~(name sym) ~update-fn ~parents ~oblivious?)))

; =======================================
; = Synchronized multi-cell evaluations =
; =======================================
(defn not-waiting? [cell-val] 
    "Determines whether a cell is either up-to-date or oblivious."
    (let [status (cell-val-status cell-val)]
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
(defn evaluate [& cells]
    "Updates the cells, waits for them to compute, and returns their vals."
    (let [        
          latch (java.util.concurrent.CountDownLatch. (count (filter cell-waiting? cells)))
          watcher-adder (fn [cell] (add-watch cell latch unlatching-watcher))
          watcher-remover (fn [cell] (remove-watch cell latch))]
        (do
            (map-now watcher-adder cells)            
            (apply update cells)             
            (.await latch)
            (map-now watcher-remover cells)
            (map deref-cell cells))))

(defn force-update [& cells]
    "Forces the cells to update and returns them immediately."
    (do
        (apply force-need-update cells)
        (apply await cells)
        (apply update cells)))
                
(defn force-evaluate [& cells]
    "Forces the cells to update, waits for them and returns their vals."
    (do 
        (apply force-need-update cells)
        (apply await cells)
        (apply evaluate cells)))

; ============================
; = Change cell dependencies =
; ============================

(defn conditional-map-replace [old-map old-key new-key dissoc-cond assoc-cond]
    "Utility function for switching parents."
    (let [dissoc-map (if dissoc-cond (dissoc old-map old-key) old-map)
            val (deref-or-val new-key)]
        (if assoc-cond 
            (if (up-to-date? val) (assoc dissoc-map new-key (extract-val val)) dissoc-map)
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
    (with-meta (if (= (cell-val-status cell-val) :oblivious) cell-val needs-update-val)
        (assoc old-meta 
            :parents parents
            :agent-parents agent-parents 
            :id-parent-vals id-parent-vals 
            :n-id-parents n-id-parents))))
            
(defn replace-parent [cell old-parent new-parent]
    "Replaces a cell's parent. Sets the cell's val to needs-update, or leaves
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