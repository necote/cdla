"""Script to extract CDLA tweets and convert to html."""


import os
import json
import argparse
from datetime import datetime, timedelta

import twitter

import credentials


def get_favorites(save_name):
    """Saves the most recent 200 favorited tweets to `save_name`."""

    api = twitter.Api(
        consumer_key=credentials.consumer_key,
        consumer_secret=credentials.consumer_secret,
        access_token_key=credentials.access_token_key,
        access_token_secret=credentials.access_token_secret
    )

    # Get 200 most recent favorites (from cunyarchive twitter account)
    favorites = api.GetFavorites(
        screen_name="cunyarchive",
        count=200,
        since_id=None,
        max_id=None,
        include_entities=True,
        return_json=True
    )

    # Save favorites data as json
    with open(save_name, 'w') as save_file:
        json.dump(favorites, save_file)

    return favorites


def get_twarc_data(until, since, save_name):
    """Download the twarc data (hastags, cuny school accounts, school presidents)"""
    
    hash_search_terms = "(#cancelcuny OR #closecuny OR #closeCUNYlibraries OR #CUNYCovid19 OR (#cuny AND (#covid19 OR #corona OR #coronavirus OR #covidcampus)))"
    presidents_search_terms = "from:CUNYProvost OR from:Vgbcc1 OR from:HunterPresident OR from:PresidentFritz OR from:JohnJayPres OR from:DrBJEanes OR from:MitchelWallerst OR from:LehmanPresident OR from:DrRudyCrew OR from:president_BCC OR from:DrTimLynch OR from:CUNYKCCPRES OR from:ChancellorCUNY OR from:CUNY OR from:BCPresAnderson OR from:DeanMaryLuBilek OR from:sarah_bartlett OR from:DeanPearl OR from:MohandesDean"
    schools_search_terms_1 = "from:bmcc_cuny OR from:BCCcuny OR from:GuttmanCC OR from:HostosCollege OR from:CUNYkcc OR from:LaGuardiaCC OR from:QCC_CUNY OR from:BaruchCollege OR from:BklynCollege411 OR from:csinews OR from:Hunter_College OR from:JohnJayCollege OR from:LehmanCollege OR from:NewsatMedgar OR from:CityTechNews OR from:QC_News OR from:CityCollegeNY OR from:YorkCollegeCUNY OR from:newmarkjschool OR from:GC_CUNY OR from:CUNYSPH"
    schools_search_terms_2 = "from:CUNYSLU OR from:CUNYLaw OR from:CUNYSPS OR from:macaulayhonors OR from:cunyarchive"
    text_search_terms = "(CUNY OR cuny)"

    until_search_terms = f'until:{until}'
    since_search_terms = f'since:{since}'

    for ix, terms in enumerate((hash_search_terms, presidents_search_terms, schools_search_terms_1, schools_search_terms_2, text_search_terms)):
        search_query = f"'({terms}) {until_search_terms} {since_search_terms}'"
        if ix == 0: 
            os.system(f"twarc search {search_query} > {save_name}")
        else:
            os.system(f"twarc search {search_query} >> {save_name}")

    # Load the twarc data
    with open(save_name, "r") as f:
        twarc = [json.loads(line) for line in f.readlines()]

    return twarc

def _combine_data_with_old(favorites, twarc, old):
    """Same concept as `combine_data` but combines the old data also."""

    # Make a dictionary organized by tweet id for favorite data and twarc data
    favorites_dict = {tweet["id"]: tweet for tweet in favorites}
    twarc_dict = {tweet["id"]: tweet for tweet in twarc}
    old_dict = {tweet["id"]: tweet for tweet in old}

    # Combine the favorite and twarc tweet ids into a unique sorted list
    favorites_keys = list(favorites_dict.keys())
    twarc_keys = list(twarc_dict.keys())
    old_keys = list(old_dict.keys())
    id_list = sorted(list(set(favorites_keys).union(set(twarc_keys).union(set(old_keys)))))

    combined = []
    for tweet_id in id_list:
        try:
            combined.append(old_dict[tweet_id])
        except KeyError:
            try:
                combined.append(twarc_dict[tweet_id])
            except KeyError:
                combined.append(favorites_dict[tweet_id])

    return combined

def combine_data(favorites, twarc, old=None):
    """Combines the favorited tweets, twarc data, and previously combined data."""
    
    # Combines all three if needed
    if old is not None:
        return _combine_data_with_old(favorites, twarc, old)
    
    # Make a dictionary organized by tweet id for favorite data and twarc data
    favorites_dict = {tweet["id"]: tweet for tweet in favorites}
    twarc_dict = {tweet["id"]: tweet for tweet in twarc}

    # Combine the favorite and twarc tweet ids into a unique sorted list
    favorites_keys = list(favorites_dict.keys())
    twarc_keys = list(twarc_dict.keys())
    id_list = sorted(list(set(favorites_keys).union(set(twarc_keys))))

    combined = []
    for tweet_id in id_list:
        try:
            combined.append(twarc_dict[tweet_id])
        except KeyError:
            combined.append(favorites_dict[tweet_id])

    return combined

    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--days",
        dest="n_days",
        default=1,
        type=int,
        choices=[1, 2, 3, 4, 5, 6, 7],
        help="Number of days for Twitter extraction from twarc."
    )
    parser.add_argument(
        "--old",
        dest="old_combined",
        default=None,
        type=str,
        help="The most recently saved data set to combine with the data being extracted."
    )
    parser.add_argument(
        "-c", "--convert",
        dest="convert",
        action="store_true",
        help="Converts the combined data to html if flag is passed."
    )
    args = parser.parse_args()

    # Get today's and yesterday's dates
    today = datetime.now()
    past = today - timedelta(args.n_days)

    # Convert from datetime to string
    today_string = datetime.strftime(today, "%d%B%Y")
    past_string = datetime.strftime(past, "%d%B%Y")

    # Make the combined file name
    today_combined = f"tweets_{today_string}.jsonl"

    # Make the folder names
    favorites_folder = "cunyarchive_favorites_api_data"
    twarc_folder = "cuny_covid19_twarc_data"
    combined_folder = "combined_twitter_data"

    # Where to save favorites gathered today
    today_favorites_file = os.path.join(favorites_folder, f"favorites_cuny_{today_string}.json")
    favorites = get_favorites(today_favorites_file)
    print("Favorites extracted")

    # Where to save the combined data
    combined_data_file = os.path.join(combined_folder, today_combined)

    # The days to limit the twarc search to
    until = datetime.strftime(today, "%Y-%m-%d")
    since = datetime.strftime(past, "%Y-%m-%d")

    # twarc data save name
    twarc_save_name = os.path.join(twarc_folder, f"tweets_cuny_{past_string}.jsonl")
    twarc = get_twarc_data(until, since, twarc_save_name)
    print("twarc data extracted")

    #Combine the data
    previous_combined = None
    if args.old_combined is not None:
        with open(args.old_combined, "r") as f:
            previous_combined = [json.loads(line) for line in f.readlines()]
    
    combined_data = combine_data(favorites, twarc, previous_combined)
    print(f"Data combined: {len(combined_data):,.0f} tweets")
    
    # Save combined list to .jsonl for using in DocNow (twarc's) formatted basic html page
    with open(combined_data_file, "w") as f:
        for tweet in combined_data:
            tweet = json.dumps(tweet)
            f.write(f"{tweet}\n")

    if args.convert:
        # Convert the whole data set to an html file
        utils_file = "twarc/utils/wall.py"
        convert_command = f'{utils_file} {combined_data_file} > {combined_data_file.replace(".jsonl", ".html")}'
        os.system(convert_command)
        print("Converted combined data to html")
    
    print("Done")


if __name__ == "__main__":
    main()
