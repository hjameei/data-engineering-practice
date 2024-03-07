import pandas as pd
import aiohttp
import os
import aiofiles
import asyncio
import concurrent.futures
from bs4 import BeautifulSoup


url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
date = '2024-01-19 09:49'

async def fetch_url(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()
        
async def fetch_filenames(url, text):
    contents = await fetch_url(url)
    soup = BeautifulSoup(contents, "html.parser")
    trs = [tr for tr in soup.find_all("tr") if any(text in td.get_text() for td in tr.find_all("td"))]
    files = [td.find('a').get('href') for tr in trs for td in tr.find_all('td') if td.find('a')]
    
    return(files)

def create_dir_downloads():
    #create download directory
    dir_proj2 =  os.getcwd()
    dir_proj2_local = os.path.join(dir_proj2, "Exercises/Exercise-2")
    if os.path.exists(dir_proj2_local):
        dir_proj2 = dir_proj2_local

    dir_proj2_download = os.path.join(dir_proj2, "downloads")
    if not os.path.exists(dir_proj2_download):
        os.mkdir(dir_proj2_download)
    return dir_proj2_download


async def download_file(uri, dir_download):
    filename = str.split(uri, "/")[-1]
    filepath = os.path.join(dir_download, filename)
    async with aiohttp.ClientSession() as session:
        async with session.get(uri) as response:
            assert response.status == 200

            async with aiofiles.open(filepath, 'wb') as file:
                async for data, _ in response.content.iter_chunks():
                    await file.write(data)    



def main():
    
    # create download directory
    dir_proj2_download = create_dir_downloads()

    # fetch filenames to be downloaded
    files_to_download = asyncio.run(fetch_filenames(url, text = date))

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        for filename in files_to_download:
            # check if file is already downloaded
            if os.path.exists(os.path.join(dir_proj2_download, filename)):
                continue

            #download the files
            try:
                uri = url + filename
                executor.submit(asyncio.run, download_file(uri, dir_proj2_download))
            except AssertionError as error:
                print("Invalid URI: {}".format(uri))

    path_files = [ os.path.join(dir_proj2_download,filename) for filename in files_to_download]

    dfs = []
    for filename in path_files:
        temp_df = pd.read_csv(filename, low_memory=False)
        dfs.append(temp_df)
    
    df = pd.concat(dfs, ignore_index=True)
    df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')
    max_temp_row = df.loc[df['HourlyDryBulbTemperature'].idxmax()]
    print(max_temp_row)


if __name__ == "__main__":
    main()
