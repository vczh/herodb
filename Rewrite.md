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
    * First page is index page (basically empty, first entry of anything is written to the hardcoded slot)
        * Usage bitmap page (format to be decided)
        * Log page ring-linked list (format to be decided)
        * Free page linked list (format to be decided)
        * First pages of tables for definitions
            * Inside schema table it stores entries of each table
            * Inside index table it stores entries of each index
            * All information will be loaded to memory after a database is online
    
