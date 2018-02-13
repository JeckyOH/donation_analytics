"""
This module is the multiple threads handler module.
Why use multithread?
Just give more scalability for future.
"""

import sys
from threading import Thread
if sys.version_info[0] == 2:
    from Queue import Queue
else:
    from queue import Queue
from avltree import avltree
from math import ceil

# Here just use a global variable to implement a kind of Singleton Pattern.
# Handler Thread is globally unique.
handler_thread = None


def handler(data, percentage, usrargs):
    """
    This function is like the export function in C++, which is called by outer interface.
    Args:
      data: input data. Format: {"cmte_id" : string,
                                 "zipcode" : string,
                                 "name" : string,
                                 "transaction_year" : int,
                                 "transaction_amt" : float}

      percentile: (Float) given percentile to be calculated.
      usrargs: (Object) object of user defined arguments.
    """
    global handler_thread
    if not handler_thread:
        handler_thread = HandlerThread(usrargs.output_path, percentage)
    # The only thing to do : Push the data into the queue of handler thread.
    handler_thread.add_task(data)

def clean():
    """
    This function should be called at the end to wait for completion of handler thread.
    """
    global handler_thread
    if not handler_thread:
        return
    handler_thread.wait_complete()
    handler_thread.clean()


class HandlerThread(Thread):
    """ This class derives from threading.Thread class and is responsible for dealing with input data.

    The thread will store input data and calculate percentile for every input data.

    Attributes:
      task_queue: The queue of input data. Main thread feeds data into this queue and this thread consumes these data.

      donation_date: Store transaction date of different donors.
                     Dictionary of { identifier : date }, in which identifier := 'NAME|ZIPCODE'

      contribution_info: Store contribution information which is indentified by "CMTE_ID|ZIPCODE|YEAR"
                         Dictionary of {
                                            "CMTE_ID|ZIPCODE|YEAR" :
                                               {
                                                "amount": TRANSACTION_AMT,
                                                "transactions": AvlTree of Transactions
                                               }
                                        }

      percentile: (Float) Given percentile to be calculated.
    """
    def __init__(self, output_file_path, percentile):
        """ Constructor of transaction handler thread.
        Args:
          output_file_path: The path of output file. According to Challenge Instructions, it should be 'project_path/output/repeat_donors.txt'.
          percentile: (Float) Given percentile to be calculated.
        """
        Thread.__init__(self)
        self.task_queue = Queue()
        self.donation_date = {}
        self.contribution_info = {}
        self.output_file = open(output_file_path, 'w')
        self.percentile = percentile
        self.daemon = True
        self.start()

    def add_task(self, item):
        """ Push data into task queue.  """
        self.task_queue.put(item)

    def wait_complete(self):
        self.task_queue.join()

    def handler(self, data):
        """ This is the single thread handler.
            Args:
               data: input data. Format: {"cmte_id" : string,
                                          "zipcode" : string,
                                          "name" : string,
                                          "transaction_year" : int,
                                          "transaction_amt" : float}
        """
        # If this is not repeat donor, just add/update its donation date.
        donor_id = '%s|%s' % (data["name"], data["zipcode"])
        if donor_id not in self.donation_date or self.donation_date[donor_id] >= data["transaction_year"]:
            self.donation_date[donor_id] = data["transaction_year"]
            return

        # If this is repeat donor. Update contribution information which is identified by this combination of receipient id, zipcode and year.
        contribution_id = '%s|%s|%s' %(data["cmte_id"], data["zipcode"], data["transaction_year"])
        if contribution_id not in self.contribution_info:
            self.contribution_info[contribution_id] = {"total_amt": 0, "transactions": avltree()}

        # Update total amount of donation.
        self.contribution_info[contribution_id]["total_amt"] += data["transaction_amt"]

        # Note: Again, note that I use AVL tree to store amount of donations and make them self-sorted.
        self.contribution_info[contribution_id]["transactions"].append(data["transaction_amt"])

        # Use Nearest-Rank Method to calculate given percentile.
        # percentile_index = (percentage / 100.0) * total_number_of_sample - 1.
        # percentile = AVL_tree_of_donations[percentile_index].
        transaction_number = len(self.contribution_info[contribution_id]["transactions"])
        percentile_index = int(ceil((self.percentile / 100.0) * transaction_number)) - 1
        percentile = int(round(self.contribution_info[contribution_id]["transactions"][percentile_index]))
        self.output_file.write('%s|%d|%d|%d\n' %(contribution_id, percentile, self.contribution_info[contribution_id]["total_amt"], transaction_number))

    def run(self):
        while(True):
            data = self.task_queue.get()
            self.handler(data)
            self.task_queue.task_done()

    def clean(self):
        self.output_file.close()
