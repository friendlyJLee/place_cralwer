import csv, sys, urllib.request, os, json

'''
Given the keyword, this program crawls all the places info for that keyword: address, name, ratings from Google map.
For example, it allows to crawl all the locations of walmarts in the US.

Restrictions: It uses Google MAP API, so, a user need to have Google MAP API Key, and will be charged according to Goolge MAP API price.
              For example, it is charged about $20 per 2,000 requests.
              Therefore, if we want to collect all locations of keywords in the US using all ZIP codes,
              it queries about 40,000 times (for all ZIP codes in the US). So, may cause about $800.


Usage: Important:
       You need to update <YOUR_KEY> to your real API KEY
'''

######### PLEASE UPDATE YOUR KEY ##########
YOUR_API_KEY = "<YOUR_KEY>"                     # get Google API keys and put here. Exposing this key causing problem, so should be managed safely
                                                    #     price per query: $15 for 1000 queries. $300 credit for the first user

# INPUT ZIP FILES
US_ZIP_CODE_LATITUDE_CSV = "us-zip-code-latitude-and-longitude.csv"

# OUTPUT FILES
OUTPUT_FOLDER = 'output'
TOTAL_SUMMARY_FILE = 'summary.csv'
RAW_OUTPUT_FILE = '{ZIP}_response.txt'
LOG_FILE = "failed_list.txt"

# GOOGLE API QUERY 
QUERY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?name={KEYWORD}&location={LOCATION}&radius={RADIUS}&key={KEY}"
RADIUS = 10000


zipLocations = []


def crawlLocationFrom(keyword, start_row, end_row):
    # Get all zip for lati, long
    print("# Getting all latitude and longitude from %s file" % US_ZIP_CODE_LATITUDE_CSV)    
    csvfile = open(US_ZIP_CODE_LATITUDE_CSV, "r")
    csvreader = csv.reader(csvfile, delimiter=',')    
    for row in csvreader:        
        zipLocations.append((row[0], row[3] + "," + row[4], row[1] + "_" + row[2]))
    csvfile.close()
    print("# Finished processing total %d ZIP codes" % len(zipLocations))

    print("# Start to query for %d to %d row" % (start_row, end_row))
    start_row = max(start_row-1, 0)
    end_row = min(end_row, 43192)

    keyword_sanitized = ''.join(map(lambda x: x in '\\/:*?"<>|' and '_' or x, keyword))
    output_folder = OUTPUT_FOLDER + "_" +  keyword_sanitized
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    
    for index in range(start_row, end_row):

        outputFile = output_folder + "\\" + RAW_OUTPUT_FILE.format(ZIP=zipLocations[index][0] + "_" + zipLocations[index][2])
        
        query = QUERY_URL.format(KEYWORD=keyword, RADIUS = RADIUS, LOCATION = zipLocations[index][1], KEY=YOUR_API_KEY)
        print("[REQUEST ROW %d] ZIPCODE: %s, CITY: %s" % (index+1, zipLocations[index][0],  zipLocations[index][2]))
        if (os.path.exists(outputFile)):
            print(" * SKIP THIS. Already we have result for this ZIP\n")
            continue

        
        response = urllib.request.urlopen(query)
        data = response.read()

        logFile = output_folder + "\\" + LOG_FILE
        
        if (response.status != 200):
            print(" * ERROR: %d\n" % response.status)
            fp = open(logFile, "a")
            fp.write("  FAILED [REQUEST ROW %d] ZIPCODE: %s, CITY: %s" % (index+1, zipLocations[index][0],  zipLocations[index][2]))
            fp.write("  ERROR: %d\n" % response.status)
            fp.write("  " + str(data) + "\n\n")
            fp.close()
            continue        

        result_dict = json.loads(data)        
        if (result_dict["status"] != 'OK' and result_dict["status"] != 'ZERO_RESULTS'):
            print(" * ERROR: %s\n" % result_dict["status"])
            fp = open(logFile, "a")
            fp.write("  FAILED [REQUEST ROW %d] ZIPCODE: %s, CITY: %s" % (index+1, zipLocations[index][0],  zipLocations[index][2]))
            fp.write("  " + str(data) + "\n\n")
            fp.close()
            continue

        
        fp = open(outputFile, "w", encoding='utf-8')
        fp.write(json.dumps(result_dict, indent=4))
        fp.close()        

        next_token = result_dict.get("next_page_token")
        if (next_token):
            print("  * WARN TODO: next_token: %s\n" % next_token)
            fp = open(logFile, "a")
            fp.write("  WARN. RESULT IS MORE THAN 20 [REQUEST ROW %d] ZIPCODE: %s, CITY: %s\n" % (index+1, zipLocations[index][0],  zipLocations[index][2]))
            fp.write("      - MIGHT NEED TO MORE QUERY for this location")
        else:
            print("  * SUCCESS\n")
            


if __name__=="__main__":
    if len(sys.argv) != 4:
        print("\nUsage: python place_crawler.py <keyword> <start_row> <end_row>")
        print("\tkeyword: search keyword that you want find. (e.g, kumon")
        print("\tstart_row: 2~43192. row from csv file where you want to start")
        print("\tstart_row: 2~43192. row from csv file where you want to stop")
        print("\nExample:")
        print("\tpython place_crawler.py kumon 2 43192\t;query all kumon locations for 2~43192 row zipcodes")          
        sys.exit(-1)

    if YOUR_API_KEY == "<YOUR_KEY>":
        print("\nIMPORTANT: You didn't update GOOGLE API KEY in place_crawler.py file.")
        print("  Please edit and replace <YOUR_KEY> string with your GOOGLE API KEY in place_crawler.py")
        sys.exit(-1)

    print("# Trying with following info")
    print("  * keyword: " + sys.argv[1])
    print("  * start_row: " + sys.argv[2])
    print("  * end_row: " + sys.argv[3])
    while True:
        answer = input("Correct and Keep going? (y/n): ")
        if (answer == 'N' or answer == 'n'or answer == 'no'): sys.exit(0)
        if (answer == 'Y' or answer == 'y' or answer == 'yes'): break
                           

    crawlLocationFrom(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    
          
                    
    







