import aiohttp
import os
import aiofiles
import asyncio
import zipfile
import concurrent.futures

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

async def uri_downloader(uri, dir_download):
    filename = str.split(uri, "/")[-1]
    filepath = os.path.join(dir_download, filename)
    async with aiohttp.ClientSession() as session:
        async with session.get(uri) as response:
            assert response.status == 200

            async with aiofiles.open(filepath, 'wb') as file:
                async for data, _ in response.content.iter_chunks():
                    await file.write(data)            

        with zipfile.ZipFile(filepath, 'r') as zeip_ref:
            zeip_ref.extractall(dir_download)
        os.remove(filepath)
    

def create_dir_downloads():

    #create download directory
    dir_proj1 =  os.getcwd()
    dir_proj1_local = os.path.join(dir_proj1, "Exercises/Exercise-1")
    if os.path.exists(dir_proj1_local):
        dir_proj1 = dir_proj1_local

    dir_proj1_download = os.path.join(dir_proj1, "downloads")
    if not os.path.exists(dir_proj1_download):
        os.mkdir(dir_proj1_download)
    return dir_proj1_download

def main():

    dir_proj1_download = create_dir_downloads()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for uri in download_uris:
            try:
                executor.submit(asyncio.run, uri_downloader(uri, dir_proj1_download))
            except AssertionError as error:
                print("Invalid URI: {}".format(uri))

if __name__ == "__main__":
    main()
