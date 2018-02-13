"""
This module is the single thread handler module.
"""

from avltree import avltree
from math import ceil

donation_date = {}  # Store transaction date of different donors.
                    # Format:
                    #    dictionary of { identifier : date }, in which identifier := 'NAME|ZIPCODE'

contribution_info = {}  # Store contribution information which is indentified by "CMTE_ID|ZIPCODE|YEAR"
                        # Format:
                        #   Dictionary of {
                        #                    "CMTE_ID|ZIPCODE|YEAR" :
                        #                       {
                        #                        "amount": TRANSACTION_AMT,
                        #                        "transactions": AvlTree of Transactions
                        #                       }
                        #                  }

output_file = None  # Store object of output file.

def handler(data, percentile, usrargs):
    """ This is the single thread handler.
    Note, the purpose of extracting this function from main module is to loose the connection between input module and handler module.
    Args:
      data: input data. Format: {"cmte_id" : string,
                                 "zipcode" : string,
                                 "name" : string,
                                 "transaction_year" : int,
                                 "transaction_amt" : float}

      percentile: (Float) given percentile to be calculated.
      usrargs: (Object) object of user defined arguments.
    """
    global donation_date
    global contribution_info
    global output_file

    if not output_file:
        output_file = open(usrargs.output_path, 'w')

    # If this is not repeat donor, just add/update its donation date.
    donor_id = '%s|%s' % (data["name"], data["zipcode"])  # Donors are identified by combination of their names and zipcodes.
    if donor_id not in donation_date or donation_date[donor_id] >= data["transaction_year"]:
        donation_date[donor_id] = data["transaction_year"]
        return

    # If this is repeat donor. Update contribution information which is identified by this combination of receipient id, zipcode and year.
    contribution_id = '%s|%s|%s' %(data["cmte_id"], data["zipcode"], data["transaction_year"])
    if contribution_id not in contribution_info:
        contribution_info[contribution_id] = {"total_amt": 0, "transactions": avltree()}

    # Update total amount of donation.
    contribution_info[contribution_id]["total_amt"] += data["transaction_amt"]

    # Note: Again, note that I use AVL tree to store amount of donations and make them self-sorted.
    contribution_info[contribution_id]["transactions"].append(data["transaction_amt"])

    # Use Nearest-Rank Method to calculate given percentile.
    # percentile_index = (percentage / 100.0) * total_number_of_sample - 1.
    # percentile = AVL_tree_of_donations[percentile_index].
    transaction_number = len(contribution_info[contribution_id]["transactions"])
    percentile_index = int(ceil((percentile / 100.0) * transaction_number)) - 1
    percentile = int(round(contribution_info[contribution_id]["transactions"][percentile_index]))

    output_file.write('%s|%d|%d|%d\n' %(contribution_id, percentile, contribution_info[contribution_id]["total_amt"], transaction_number))

def clean():
    output_file.close()
