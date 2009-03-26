lazy-agent : Concurrent, lazy cells for Clojure
===============================================

Implements two types of agent-based 'cells' for Clojure: lazy cells and oblivious cells. Both update concurrently, with respectably efficient scheduling, and avoid unnecessarily repeating cell updates.

Usage
-----

Lazy cells depending on a ref can be created as follows. Their update functions include a time delay to simulate long computation::

    (defn sleeping [fun]
        (fn [& x] (do (Thread/sleep 1000) (apply fun x))))
    (def x (ref 10))
    (def-cell a (sleeping /) [1 x])
    (def-cell b (sleeping +) [2 3])

If you deref a lazy cell, you'll see a map::

    user=> @a
    {:val nil, :status :needs-update}
    user=> @b
    {:val nil, :status :needs-update}

The key ``:val`` gives the value of the cell, if it is available. ``:status`` may be: 

* ``:needs-update``
* ``:updating``
* ``:up-to-date``
* ``:error`` or
* ``:oblivious``

To update a group of cells asynchronously, do:: 

    user=> (update a b) 

To update and wait for the values, do (note the concurrency, ``a`` and ``b`` take one second each to update):: 

    user=> (time (evaluate a b))
    "Elapsed time: 1005.231 msecs"
    (1/10 5)
    
When a lazy cell's ancestor changes, its value changes to ``{:value nil :status :needs-update}``::

    user=> (dosync (ref-set x 11))
    11
    user=> @a
    {:val nil, :status :needs-update}
    
It does not compute its new value until it receives a message instructing it to do so.

Lazy cells are guaranteed to update only once per 'update' or 'evaluate' call. They will not update until all of their parents are up-to-date.

Oblivious cells
----------------

Oblivious cells can be created by passing an optional argument to ``def-cell``. ``:oblivious``::

    (def x (ref 10))
    (def-cell a (sleeping /) [1 x])
    (def-cell b (sleeping +) [2 3])
    (def-cell c (sleeping +) [a b] true)

Oblivious cells are even lazier than lazy cells. If an ancestor subsequently changes, the cell will not do anything:: 

    user=> (time (evaluate a b c))
    "Elapsed time: 2008.252 msecs"
    (1/10 5 51/10)
    user=> (dosync (ref-set x 11))
    11
    user=> @a
    {:val nil, :status :needs-update}
    user=> @c
    {:val 51/10, :status :oblivious}
    user=> (evaluate a)
    (1/11)
    user=> @c
    {:val 51/10, :status :oblivious}
    
It needs to receive a ``force-need-update`` message to change state to ``{:value nil :status :needs-update}``. After that, it behaves like a lazy cell until the next time it updates its value, at which point its status is reset to ``:oblivious``::

    user=> (force-needs-update c)
    nil
    user=> @c
    {:val nil, :status :needs-update}
    user=> (evaluate c)
    (56/11)
    user=> @c
    {:val 56/11, :status :oblivious}

Exceptions
----------

If a cell encounters an exception when computing, that exception is recorded in its value and propagated to all its descendants::

    (def x (ref 10))
    (def-cell a (sleeping /) [1 x])
    (def-cell b (sleeping +) [2 3])
    (def-cell c (sleeping +) [a b] true)
    (def-cell d (sleeping +) [c a 3])
    
    user=> (time (evaluate a b c d))
    "Elapsed time: 3007.395 msecs"
    (1/10 5 51/10 41/5)
    user=> (dosync (ref-set x 0))
    0
    user=> (evaluate a d)  
    ({:self #<ArithmeticException java.lang.ArithmeticException: Divide by zero>} {#<Agent@85e41d: {:val {:self #<ArithmeticException java.lang.ArithmeticException: Divide by zero>}, :status :error}> #<ArithmeticException java.lang.ArithmeticException: Divide by zero>})
    
Exceptions are stored in maps to make it easy to figure out the cell in which they originated::

    user=> ((@a :val) :self)
    #<ArithmeticException java.lang.ArithmeticException: Divide by zero>
    user=> ((@d :val) a)
    #<ArithmeticException java.lang.ArithmeticException: Divide by zero>

Oblivious descendants ignore exceptions in their ancestors::

    user=> @c
    {:val 51/10, :status :oblivious}

Exceptions are automatically cleared when possible::

    user=> (dosync (ref-set x 2)) 
    2
    user=> @a
    {:val nil, :status :needs-update}
    user=> @b
    {:val 5, :status :up-to-date}
    user=> @c
    {:val 51/10, :status :oblivious}
    user=> @d
    {:val nil, :status :needs-update}
    user=> (evaluate a d)
    (1/2 43/5)
    

License
-------

Copyright (c) Anand Patil, 2009. Licensed under Creative Commons BY-SA, see LICENSE.