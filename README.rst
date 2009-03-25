lazy-agent : concurrent, lazy cells for Clojure
===============================================

Implements two types of agent-based 'cells' for Clojure: lazy cells and oblivious cells. These complement the auto-agents available in Clojure Contrib. Both allow for concurrent cell updates with respectably efficient scheduling and avoid unnecessarily repeating cell updates.


Usage
-----

Lazy cells dependent on a ref can be created as follows::

    (def x (ref 10))
    (def-cell a (sleeping /) [1 x])
    (def-cell b (sleeping +) [2 3])

If you deref a lazy cell, you'll see a map::

    user=> @a
    {:val nil, :status :needs-update}
    user=> @b
    {:val nil, :status :needs-update}

``:status`` may be: 

* ``:needs-update``
* ``:updating``
* ``:up-to-date``
* ``:error`` or
* ``:oblivious``

If a cell is up-to-date or oblivious, ``:value`` gives the value of the cell.

When a lazy cell's ancestor changes, its value changes to ``{:value nil :status :needs-update}`` but it does not compute its new value until it receives a message instructing it to do so. To send the update message to a group of cells, do:: 

(update a b c d e) 

To send the update message and wait for the values, do:: 

(evaluate a b c d e)

Lazy cells are guaranteed to update only once per 'update' or 'evaluate' call. They will not update until all of their parents are up-to-date.

Oblivious cells
----------------

Oblivious cells can be created by passing an optional argument to ``def-cell``. When such an cell is up-to-date, its status is ``:oblivious``. 

Oblivious cells are even lazier than lazy cells. If an ancestor subsequently changes, the cell will not do anything. It needs to receive a ``force-need-update`` message to change state to ``{:value nil :status :needs-update}``. After that, it behaves like a lazy cell until the next time it updates its value, at which point its status is reset to ``:oblivious``.




Author
------

Anand Patil

License
-------

Copyright (c) Anand Patil, 2009. Licensed under Creative Commons BY-SA, see LICENSE.