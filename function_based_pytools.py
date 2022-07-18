"""
Created by: Alissa Graff, on detail from 1/16/22 to 7/16/22, reach out at graffalissa@gmail.com with specific questions - happy to help!

This program utilizes a reference ID to download the data and metadata from an uploaded data package. 
The process to achieve this is as follows:
1. identify reference number for data package - user should change the "test ref" variable to their desired reference ID of interest. Or, in the next step, when utilizing the by_referenceID function, the user can simply input a reference ID as the parameter for that function. 
2. utilize the by_referenceID function to gather the API dictionary metadata for the package. This function takes a refrence ID as an input and returns the dictionary, so it should be saved to a variable, as seen in the "GET REFID DATA" section.
3. utilize the download_package function to select the download link from the API dictionary metadata for the datapackage. This will start the download process for the files and save them to the directory set by the user in the download_destination variable at the beginning of the script.
- NOTE: Currently, no data downloads to the files due to an authentication issue that has not been resolved. This should be the primary issue to resolve next. Currently the files downloaded will show a garbled error message that notes that authentication failed. 
"""

# import statements
import json
import os
import requests

# unzipped data package - coral dataset from SFCN titled "SFCN Coral Reef Video dataset for U.S. Virgin Islands parks (1999-2020) - OData Test" - contains CSV and XML separate files:
test_ref = 2293662

# restricted access data package - uncomment if trying to test:
# test_ref = 672874

""" This should be changed by the user if desired to download to a different location. Otherwise will download to the folder of this file in a 'packages' folder. """
download_destination = "packages/" + str(test_ref)
results_diction = {}

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

# results structure:
# ReferenceSearchResults is a dictionary
# items and pageDetail are the keys
# items is a list of dictionaries
# pageDetail is a dictionary

# This function allows a user to utilize the QuickSearch API function - searching for a keyword or number to retrieve a list of results from the API. REferenceSearchResults is a dictionary that contains multiple dictionaries of results. If searching for one reference ID number, there should only be one result. 
def quick_search(search_term):
    baseurl = "https://irmaservices.nps.gov/datastore/v4/rest/"
    query_category = "QuickSearch?"
    ReferenceSearchResults = {}
    ReferenceSearchResults["items"] = []
    ReferenceSearchResults["pageDetail"] = []

    params_diction = {}
    params_diction["q"] = search_term

    unique_ident = params_unique_combination(baseurl, params_diction, query_category)
    
    response = requests.get(unique_ident)
    print("response:", response)
    python_object = json.loads(response.text)
    results_diction[unique_ident] = python_object

    ReferenceSearchResults["items"] = (results_diction[unique_ident]["items"])
    
    return ReferenceSearchResults

print("*********GET SEARCH DATA**********")
# shows an example of what results from the quick_search function using the reference ID noted above. 
# This can also be used to test the quick_search function generally, just use a different input - it takes a string as a query and will return all results that match.
search_data_test = quick_search(test_ref)
print("search data test results:", search_data_test)

# Function to return metadata from a referenceID search for digital file link in order to download
# This function takes the reference ID of interest as its input and will return the API metadata for this item. 
def by_referenceID(refID):
    baseurl = "https://irmaservices.nps.gov/datastore/v4/rest/Reference/"
    query_category = "/DigitalFiles"
    refID_results = {}

    params_diction = {}
    params_diction["referenceID"] = refID

    unique_ident = params_unique_combination(baseurl, params_diction, query_category)
    
    response = requests.get(unique_ident)
    print("response:", response)

    if response.status_code !=200:
        print("Failure! See printed response code.")
    else:
        python_object = json.loads(response.text)
        results_diction[unique_ident] = python_object

    refID_results = results_diction[unique_ident]

    return refID_results

print("*********GET REFID DATA**********")
# obtaining the API metadata for the reference ID and saving it in the variable refID_data_test -- which will then be used in the download_package function to actually download the files and data.
refID_data_test = by_referenceID(test_ref)
print("refid data: ", refID_data_test)


print("*****TRYING DOWNLOADPACKAGE**********")
# This function will download the data package as two separate files (just as it is stored). The current issue here is authentication - this issue has not yet been resolved. Currently what is written to files is garbled error messages because there is no authentication process to download the actual data. This is an issue that should be resolved and added. 
def download_package(ref_metadata):
    if not os.path.isdir(download_destination):
        print("making datapackage directory")
        os.makedirs(download_destination)
        print("directory: ", os.listdir(download_destination))

    req1 = requests.get(ref_metadata[0]['downloadLink'])
    req2 = requests.get(ref_metadata[1]['downloadLink'])
    print("downloading the zip file")

    with open(download_destination + "/" + str(ref_metadata[0]['fileName']), "wb") as f:
        print("opened file destination + writing File 1 contents")
        f.write(req1.content)
        f.close()
        print("file1 downloaded here: ", str(download_destination))

    with open(download_destination + "/" + str(ref_metadata[1]['fileName']), "wb") as f2:
        print("opened file destination + writing File 2 contents")
        f2.write(req2.content)
        f2.close()
        print("file2 downloaded here: ", str(download_destination))


# using this area to test out the functions
if __name__ == '__main__':
    download_package(refID_data_test)