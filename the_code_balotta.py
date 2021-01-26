from csv import reader
from datetime import datetime
from dateutil.relativedelta import relativedelta
import networkx as nx
import re


def process_citations(citation_file_path):  # Takes in input a CSV file and returns a data structure with all the data included in the CSV
    data = nx.DiGraph()                                                 # Data structure is a Directed Graph
    with open(citation_file_path, 'r', encoding='utf-8') as csvfile:
        csv_reader = reader(csvfile)                                    # csv_reader is an iterator over the lines of the CSV file, where each line is a list of strings
        next(csv_reader)                                                # Skips the first line containing the column headings
        for row in csv_reader:                                          # One row at a time is read, each row is a list of strings
            if len(row) == 4:                                           # Checks if the current row has the right amount of fields
                if row[0] not in data:                                  # row[0] is the "citing" column
                    data.add_node(row[0], creation=row[2])              # row[2] is "creation" date, and it is related to the document, so it's an attribute of the node
                if row[1] not in data:                                  # row[1] is the "cited" column
                    data.add_node(row[1], creation=find_cited_date(row[2], row[3]))  # Creates the "cited" node and calculates its creation date
                data.add_edge(row[0], row[1], timespan=row[3])          # row[3] is "timespan", that is a time difference between the two documents, so it's an attribute of the edge
    return data


def do_compute_impact_factor(data, dois, year):                         # Calculates the Impact Factor of the dois in the year
    citing_cntr = 0                                                     # Counter of citing documents (numerator)
    previous_years_dois_cntr = 0                                        # Counter of dois published in the past two years (denominator)
    year = int(year)                                                    # Makes sure that it's a number, because later we will compare it with numbers, not strings
    for doi in dois:                                                    # For each DOI in the dois set...
        if doi in data:                                                 # Checks if each requested DOI is inside the data structure

            # This part is to find into 'data' the citations all the documents in dois have received in year 'year'
            predecessor_dois = data.predecessors(doi)                   # Gets all the predecessors of the current DOI
            for citing in predecessor_dois:                             # For each node of the edges incoming to the current DOI...
                if int(data.nodes[citing]['creation'][0:4]) == year:    # Extracts the year from the creation date string of a citing document, converts it to a number and compares it to the year parameter
                    citing_cntr += 1                                    # Increases the counter of citing documents if creation date is in the required year

            # This part is to find the number of documents in dois published in the previous two years
            doi_creation_year = int(data.nodes[doi]['creation'][0:4])   # Extracts the year from the creation date of the current DOI
            if doi_creation_year == year-1 or doi_creation_year == year-2:
                previous_years_dois_cntr += 1                           # Increases the counter of documents published in the past two years

    if previous_years_dois_cntr == 0:                                   # If there are no dois in the previous two years, returns None
        return None
    return citing_cntr/previous_years_dois_cntr                         # Otherwise returns the factor


def do_get_co_citations(data, doi1, doi2):  # Returns an integer defining how many times the two input documents are cited together by other documents
    if doi1 in data and doi2 in data:
        predecessors_doi1 = set(data.predecessors(doi1))                    # Creates a set with the predecessors of doi1
        predecessors_doi2 = set(data.predecessors(doi2))                    # Creates a set with the predecessors of doi2
        citing_both = predecessors_doi1.intersection(predecessors_doi2)     # Gets only the documents the two sets have in common
        return len(citing_both)                                             # Returns the number of common elements
    else:
        return 0


def do_get_bibliographic_coupling(data, doi1, doi2):  # Returns an integer defining how many times the two input documents cite both the same document
    if doi1 in data and doi2 in data:
        successors_doi1 = set(data.successors(doi1))                  # Creates a set of edges outgoing from doi1
        successors_doi2 = set(data.successors(doi2))                  # Creates a set of edges outgoing from doi2
        cited_both = successors_doi1.intersection(successors_doi2)    # Gets only the documents the two sets have in common
        return len(cited_both)                                        # Returns the number of common elements
    else:
        return 0


def do_get_citation_network(data, start, end):  # Returns a directed graph containing all the articles involved in citations if both of them have been published within the input start-end interval (start and end included)
    start = str(start)                          # To make sure we are working with strings
    end = str(end)                              # To make sure we are working with strings
    eligible_edges = list()
    result = nx.DiGraph()
    if (start <= end) and (len(start) == len(end) == 4):                                # Some check over the passed parameters
        for from_node, to_node in data.edges():                                         # Iterates over the whole graph
            from_node_year = data.nodes[from_node]['creation'][0:4]                     # Extracts the year of the citing node
            to_node_year = data.nodes[to_node]['creation'][0:4]                         # Extracts the year of the cited node
            if (start <= from_node_year <= end) and (start <= to_node_year <= end):     # If both documents have been published in the start-end interval
                eligible_edges.append((from_node, to_node))                             # Adds the edge (and its nodes) to a list
        result.add_edges_from(eligible_edges)                                           # Populates the resulting DiGraph from the list
    return result


def do_merge_graphs(data, g1, g2):  # Returns a new graph being the merge of the two input graphs if these are of the same type (e.g. both DiGraphs). In case the types of the graphs are different, return None
    if type(g1) == type(g2):
        return nx.compose(g1, g2)
    else:
        return None


def do_search_by_prefix(data, prefix, is_citing):
    slashpos = len(prefix)        # To avoid to run the len function multiple times later
    filtered_edges = list()
    for x, y in data.edges():
        if (is_citing and x.startswith(prefix) and (x.find("/")) == slashpos) or \
                (not is_citing and y.startswith(prefix) and (y.find("/")) == slashpos):
            filtered_edges.append((x, y))
    return data.edge_subgraph(filtered_edges)


def do_search(data, query, field):      # Returns a sub-collection of citations in data where the query matched on the input field. It is possible to use wildcards in the query. If no wildcards are used, there should be a complete match with the string in query to return that citation in the results
    requery = translate_query_string_for_search(query)           # Converts the query into an evaluable expression
    filtered_edges = get_filtered_list(data, requery, field)
    return data.edge_subgraph(filtered_edges)


def do_filter_by_value(data, query, field):      # Returns a sub-collection of citations in data where the query matched on the input field. No wildcarts are permitted in the query, only comparisons
    requery = translate_query_string_for_filter(query)           # Converts the query into an evaluable expression
    filtered_edges = get_filtered_list(data, requery, field)
    return data.edge_subgraph(filtered_edges)


# ANCILLARY FUNCTIONS ################

def find_cited_date(citing_date, timespan):   # Returns the date of the cited document given the date of the citing and the timespan
    datelen = len(citing_date)
    if datelen == 7:
        citing_date += "-01"            # Adds the day to the string if citing_date has only the year and the month
    elif datelen == 4:
        citing_date += "-01-01"         # Adds month and day to the string if citing_date has only the year

    times = timespan.replace('Y', ':').replace('M', ':')[1:len(timespan)-1].replace('P', '').split(':')  # Creates a list with only the numbers of years, months (if any) and days (if any) to add or subtract
    times = [-int(item) if timespan[0] == 'P' else int(item) for item in times]                          # Switches to negative values if the timespan is positive (that is, if the cited doc has been published BEFORE the citing doc)
    citing_datetime = datetime.fromisoformat(citing_date)                           # Converts the date string to datetime format to enable operations
    cited_date = citing_datetime + relativedelta(years=times[0])                    # Sums (or subtracts) the years

    try:                                                                            # Months and days could not be specified in the timespan, so I put it inside a try rather than check before
        cited_date = cited_date + relativedelta(months=times[1])                    # Sums (or subtracts) the months
        cited_date = cited_date + relativedelta(days=times[2])                      # Sums (or subtracts) the days
    except IndexError:
        pass                                                                        # If it couldn't add the months and the days. does nothing

    return str(cited_date.date())[:datelen]       # Return a string of the same format of the citing_date (YYYY-MM-DD or YYYY-MM or YYYY)


def evaluate_query_string(requery, fieldvalue):   # Evaluates the complete query string with the field value
    try:
        result = bool(eval(requery.format(fieldvalue)))
    except:
        result = False
    return result


def get_filtered_list(data, requery, field):    # For do_search and do_filter functions, looks inside the data structure based on the search field
    filtered_edges = []
    if field == 'citing' or field == 'cited':
        filtered_edges = [(citing, cited) for citing, cited in data.edges()
                          if (evaluate_query_string(requery, citing.lower())
                              if field == 'citing' else evaluate_query_string(requery, cited.lower()))]
    elif field == 'creation':
        filtered_edges = [(citing, cited) for citing, cited in data.edges()
                          if evaluate_query_string(requery, data.nodes[citing]['creation'])]
    elif field == 'timespan':
        filtered_edges = [(citing, cited) for citing, cited, attributes in data.edges(data=True)
                          if evaluate_query_string(requery, attributes['timespan'].lower())]
    return filtered_edges


def escape_query_string(querystring):       # Escapes special characters having a meaning for RegEx syntax (e.g the dot) PRESERVING THE STARS
    escstring = ""
    parts = querystring.split("*")
    for part in parts:
        escstring += "*"+re.escape(part)
    return escstring[1:len(escstring)]      # Excludes the first asterisk that has been added


def translate_query_string_for_search(query):       # Converts the query string to a logically evaluable one containing re.search function calls
    wordlist = query.split()                        # Splits query by spaces
    operators = ['and', 'or', 'not']                # List of the only possible operators
    finaleval = ""
    for word in wordlist:
        if word not in operators:                       # For every string that is not an operator...
            word = escape_query_string(word.lower())    # escapes the characters having a meaning for RegEx syntax
            word = "^" + word.replace("*", ".*?") + "$" # Adds the characters needed by RegEx syntax
            word = 're.search("'+word+'","{0}")'        # Adds the re.search() function call and its parameters
        finaleval += ' '+word+' '                       # Adds the current token/operator to the final result
    return finaleval


def translate_query_string_for_filter(query):  # converts the query string to a logically evaluable one for do_filter function
    wordlist = query.split()                        # Splits query by spaces
    comp_op = ['<=', '>=', '==', '!=', '<', '>']    # List of operators

    i = 0                           # Index of the positions of the compare operators
    insertlist = []                 # List of the positions of the compare operators
    for el in wordlist:             # Searches the list for compare operators
        if el in comp_op:
            insertlist.append(i)    # Stores the index of the compare operator
            i += 1                  # For every compare op. I found, the index has to be incremented once more
        i += 1
    for el in insertlist:
        wordlist.insert(el, '"{0}"')                    # Inserts the parameter "{0}" WITH QUOTES at the positions found before
        wordlist[el+2] = '"' + wordlist[el+2] + '"'     # Adds quotes to the string after the operator

    return ' '.join(wordlist).lower()           # Rebuilds the query with the parameters and the quotes and everything lowercase
