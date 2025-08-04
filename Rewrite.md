* Rewrite the whole thing using Clion and VS
* Separate file mapping object from memory management
* MVCC
* Executing
    * Save query to log
    * Run the transaction parallelly
        * a transaction is canceled right away when
            * an update is requested but depended pages have been changed (including index pages)
            * another transaction is submitted, and updated depended pages of this transaction
            * deadlock happens and this transaction's cost is lower
    * Single-threaded apply transaction update (mean while updates can be queued parallelly)
        * Submit the transaction of highest cost, rollback all conflicted transactions
            * Could be optimized to pick lower cost transactions when the sum is higher to prevent from rolling-back too much cost
        * Prepare for persisting updated pages
        * Mark or remove the log
    * Need to consider about recovering when the database is shutdown at any timing
* The database need to offline so that it can
    * Changes schema
* Others unchanged

## Craft

Paged file -> Basic SQL -> object oriented database / prolog query language

* Pages
    * Index hierarchy:
        * 1st level index page stores entries to 2nd index pages, 8 bytes each
        * 2nd level index page stores entries to 3rd inddx pages, 8 bytes each
        * 3rd level index page stores allocations to data pages
            * 0 = not allocated, may be in thr linked list or may be not
            * 1 = allocated
            * all (n-3)+(n-3)+n bits form a number to the physical page id
        * 1st level index page is the first page
        * linked list of deallocated pages is the second page
            * linked list allocates pages too, when popping a page and if the last linked node has only one entry, the linked page itself is 
    * page id
        * all page sizes identical, a 2^n bytes page size set the file's maximum size to 2^(4n - 6) bytes. n >= 12
        * there are 2^(3n - 6) pages, (n-3)+(n-3)+n bits to locate an entry in each level page
        * when open a page for accessing, cheeck its availability in 3 level index pages
        * a lower level function is offered to access page without checking, for accessing page indexes
    * system pages
        * first page stores all properties, include physical page id to other system pages
        * 1st slot: second page is the 1st index page, never deallocated
        * 2nd slot: third page is the first linked node, never deallocated
        * 3rd slot: last linked node
        * 4st slot: the smallest page id that have never been used, this page will be allocated when linked list is empty
        * schema entries, transaction entries, etc
        * system pages also marked as allocated, and can be deallocated
