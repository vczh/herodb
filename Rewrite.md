* Rewrite the whole thing using Clion and VS
* Separate file mapping object from memory management
* MVCC
* No log fragment, backend run a transaction in the following sequence:
    * Create log
    * Run the transaction and fill the log
    * Save the log
    * Return success
* Backend categorizes jobs into different proprity (low - high):
    * Normal query
    * Change schema (db exclusive)
    * Memory management and file flushing (log the timestamp) (db exclusive)
    * Shut down (instance exclusive)
* Others unchanged
