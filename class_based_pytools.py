"""
Created by: Alissa Graff, on detail from 1/16/22 to 7/16/22, reach out at graffalissa@gmail.com with specific questions - happy to help!

This program utilizes an Object Oriented approach to searching for and downloading resources from the IRMA API. The user can use parameters to determine what type of query they are doing, and the functions will work together with this query.  There are currently two primary functionalities: downloading a data package based on a pre-identified reference ID, and downloading search results for a keyword or reference ID search. If the user is certain about the reference ID they are looking for, and the file is a "DigitalFile" per the API, they should use the "/DigitalFiles" parameter. 
If the user is curious to search for a specific search term through the API, the functionality will download the search results to the API. Please see the note at the bottom of this section for the caveat. 

To download a data package:
1. Instantiate the Search() object by assigning it to a variable. For instance:
search_var = Search(reference_ID, "/DigitalFiles")
Parameters:
reference_ID = takes an integer of a reference ID that the user would like to download
the second parameter identifies what kind of query this is - for data packages, use "/DigitalFiles"
2. download the data package via the download_package function. For instance:
download = search_var.download_package()
3. Be sure to call these variables in order to have the data gathered and downloaded. 

To download search results to a CSV:
1. Instantiate the Search() object by assigning it to a variable. For instance:
search_var = Search(search_term, "QuickSearch?")
Parameters:
search_term = takes any string
the second parameter identifies what kind of query this is - for quick searches, use "QuickSearch?"

2. download the search results to a CSV via the download_search_results function. For instance:
search_download = search_term.download_search_results()

3. again, be sure to call these variables for their actions to work. 

- NOTE for Data Package downloads: Currently, no data downloads to the files due to an authentication issue that has not been resolved. This should be the primary issue to resolve next. Currently the files downloaded will show a garbled error message that notes that authentication failed. 
- NOTE for QuickSearch downloads:
Currently, it is not returning all of the possible items related to the search term in the results. For instance, a QuickSearch for "Isle Royale" with this program will only return 25 results, when there are more than 4,000 reference items in DataStore that appear when searching for "Isle Royale" in the DataStore QuickSearch interface
"""




# import statements
import json
import os
import requests
import csv

# unzipped data package - coral dataset from SFCN titled "SFCN Coral Reef Video dataset for U.S. Virgin Islands parks (1999-2020) - OData Test" - contains CSV and XML separate files:
test_ref = 2293662

# restricted access data package - uncomment if trying to test:
# test_ref = 672874

""" This should be changed by the user if desired to download to a different location. Otherwise will download to the folder of this file in a 'packages' folder. """


# This function creates the unique URL to identify the target item that the user would like to retrieve from the API. If the user would like to do a QuickSearch - because they do not know the reference ID or otherwise - that should be identified as the query_cat parameter. 
def params_unique_combination(baseurl, params_d, query_cat=""):
    alphabetized_keys = sorted(params_d.keys())
    res = []

    if query_cat == "QuickSearch?":
        print("quick search identified")
        for k in alphabetized_keys:
            res.append("{}={}".format(k, params_d[k]))
        final_url = baseurl + query_cat + "&".join(res)
        print(final_url)
    else:
        print("other category identified")
        for k in alphabetized_keys:
            res.append("{}".format(params_d[k]))
        final_url = baseurl + "&".join(res) + query_cat
        print("final url:", final_url)
    return final_url

# Use the following options for type depending on what you're interested in doing:
# for doing a search, be sure to use:
# "QuickSearch?"

# for getting a specific data package whose reference ID you know, use:
# "/DigitalFiles"
class Search():
    def __init__(self, search_term, type=""):
        baseurl = "https://irmaservices.nps.gov/datastore/v4/rest/"
        query_cat = type
        params_diction = {}
        params_diction["q"] = search_term
        self.pkg_download_destination = "packages/" + str(search_term)
        # print(self.pkg_download_destination)
        self.search_download_destination = "search_results/" + str(search_term)
        results_diction = {}

        if query_cat == "/DigitalFiles":
            self.unique_ident = params_unique_combination(baseurl + "Reference/", params_diction, query_cat)
        elif query_cat == "QuickSearch?":
            self.unique_ident = params_unique_combination(baseurl, params_diction, query_cat)
        
        response = requests.get(self.unique_ident)
        print("response:", response)
        self.api_response_object = json.loads(response.text)
        results_diction[self.unique_ident] = self.api_response_object

        print("Ref Search Results:", self.api_response_object)

# This function takes the Search object as its input, specifically using the attributes of the pkg_download_destination and the api_response_object to download a data package.
# to simply see the API metadata results related to the data package, call the Search() object with the attribute api_response_object
    def download_package(self):
        if not os.path.isdir(self.pkg_download_destination):
            print("making datapackage directory")
            os.makedirs(self.pkg_download_destination)
            print("directory: ", os.listdir(self.pkg_download_destination))
        else:
            print("continuing")

        req1 = requests.get(self.api_response_object[0]['downloadLink'])
        req2 = requests.get(self.api_response_object[1]['downloadLink'])
        print("downloading the file")

        with open(self.pkg_download_destination + "/" + str(self.api_response_object[0]['fileName']), "wb") as f:
            print("opened file destination + writing File 1 contents")
            f.write(req1.content)
            f.close()
            print("file1 downloaded here: ", str(self.pkg_download_destination))

        with open(self.pkg_download_destination + "/" + str(self.api_response_object[1]['fileName']), "wb") as f2:
            print("opened file destination + writing File 2 contents")
            f2.write(req2.content)
            f2.close()
            print("file2 downloaded here: ", str(self.pkg_download_destination))

# This function takes the Search object as its input, specifically using the attributes of the search_download_destination and the api_response_object to download the results of searching for a keyword while using the API
    def download_search_results(self):
        if not os.path.isdir(self.search_download_destination):
            print("making datapackage directory")
            os.makedirs(self.search_download_destination)
            print("directory: ", os.listdir(self.search_download_destination))
        else:
            print("continuing")
        
        csv_headers = self.api_response_object['items'][0].keys()
        
        full_data_list = []

        for i in range(len(self.api_response_object['items'])):
            # csv_keys = self.api_response_object['items'][i].keys()
            ref_ID= self.api_response_object['items'][i]['referenceId']
            ref_type = self.api_response_object['items'][i]['referenceType']
            date_issue = self.api_response_object['items'][i]['dateOfIssue']
            lifecycle = self.api_response_object['items'][i]['lifecycle']
            visibility = self.api_response_object['items'][i]['visibility']
            file_count = self.api_response_object['items'][i]['fileCount']
            file_access = self.api_response_object['items'][i]['fileAccess']
            title = self.api_response_object['items'][i]['title']
            citation = self.api_response_object['items'][i]['citation']
            newest_version = self.api_response_object['items'][i]['newestVersion']

            csv_row_list = [ref_ID, ref_type, date_issue, lifecycle, visibility, file_count, file_access, title, citation, newest_version]

            full_data_list.append(csv_row_list)

            # print("printing values from item {}".format(i), csv_row_list)

        # print("full data list:", full_data_list)

        with open(self.search_download_destination +".csv",'w', newline='', encoding='utf-8') as csvfile:
            headers = csv_headers
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for i in range(len(self.api_response_object['items'])):
                writer.writerow(self.api_response_object['items'][i])
            
        
        print("done writing results to CSV at {}".format(self.search_download_destination))
        print(len(self.api_response_object['items']))



if __name__ == '__main__':

    # Example:

    # Create a Search Object (data package version):
    data_pkg_search = Search(test_ref, "/DigitalFiles")

    # Create download data package object:
    new_data_pkg_download = data_pkg_search.download_package()

    # Create a Search Object
    new_search = Search("Isle Royale", "QuickSearch?")

    # Create a download object
    download = new_search.download_search_results()

    # new_search
    # download
    print(new_search)
    print(download)