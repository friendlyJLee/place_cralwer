'''
Processing all the cralwed information in output folder. and generate one summary file

'''


from place_crawler import * 
import glob, os

def analyze(output_folder):

    # place_id: name, vicinity, business_status, rating, user_ratings_total, plus_code:compound_code, zip:(), city:(), state:()
    summary = {}

    if os.path.exists(output_folder) == False:
        print ("# There is no crawled data in folder %s" % output_folder)
        print ("# Please make sure to call 'place_crawler.py' before analysis")
        return
               
    
    for f in glob.glob("%s\\*response.txt" % output_folder):        

        zipCode, city, state, _ = os.path.basename(f).split("_")
        print ("# Process %s %s %s" % (zipCode, city, state))

        fp = open(f, "r")
        jsondata = json.load(fp)
        fp.close()

        results = jsondata['results']
        for result in results:
            place_id = result["place_id"]
            data = summary.get(place_id)
            if (data == None):
                data = []
                data.append(set([zipCode]))
                data.append(set([state]))
                data.append(set([city]))                
                data.append(result["name"])
                data.append(result["vicinity"])
                data.append(result["business_status"])
                data.append(result["rating"])
                data.append(result["user_ratings_total"])
                if (result.get('plus_code')):
                    data.append(result["plus_code"]["compound_code"])
                else:
                    data.append("")
                summary[place_id] = data
                print (" * Append New Data")
            else:
                data[0].add(zipCode)
                data[1].add(state)
                data[2].add(city)                
                print (" * Duplicated data is updated")

    print("# Finished to process. Total data: %d" % len(summary))


    # Write to file
    summary_file = output_folder + "\\" + TOTAL_SUMMARY_FILE
    fp = open(summary_file,'w', newline='', encoding='utf-8')
    fnames = ['zip', 'state', 'city', 'name', 'vicinity', 'business_status', 'rating', 'user_ratings_total', 'compound_code']
    wt = csv.writer(fp)    
    wt.writerow(fnames)
    for key, data in summary.items():
        data[0] = ' or '.join(data[0])
        data[1] = ' or '.join(data[1])
        data[2] = ' or '.join(data[2])        
        wt.writerow(data)
    fp.close()
    print("# Result is stored into %s file" %  summary_file)

if __name__=="__main__":
    if len(sys.argv) != 2:
        print("\nUsage: python sumerize_place_crawler_info.py <keyword>")
        print("\tkeyword: search keyword that you want find. (e.g, kumon")        
        print("\nExample:")
        print("\tpython sumerize_place_crawler_info.py kumon")          
        sys.exit(-1)


    keyword_sanitized = ''.join(map(lambda x: x in '\\/:*?"<>|' and '_' or x, sys.argv[1]))
    output_folder = OUTPUT_FOLDER + "_" +  keyword_sanitized
    print("# Trying with following info")
    print("  * keyword: " + sys.argv[1])
    print("  * output_folder to check: " + output_folder)    
    print("\n# Analyze all data in %s folder" % OUTPUT_FOLDER)
    print("# It will make one summary cvs file.")
    input("Press any key to continue")
    analyze(output_folder)
