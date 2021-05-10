import sys, os
import aiohttp, asyncio
import pandas as pd
class ImageInfo:
    def __init__(self, md5, url):
        self.md5 = md5
        self.url = url

    def get_filename(self):
        return f"{self.md5}.txt"

def load_urls(filename):
    ret = []
    urls=pd.read_csv(filename)['FName']
    for url in urls:
        name=url.split('/')[-1]
        #md5, url, *_ = line.strip().split("\t", 3)
        if not url.endswith(".txt"):
            continue
        ret.append(ImageInfo(name, url))
    return ret


class Spider:
    def __init__(self, save_dir, n_job=5000):
        self.save_dir = save_dir
        self.semaphore = asyncio.Semaphore(n_job)

    async def __get_content(self, url):
        async with self.semaphore:
            async with aiohttp.ClientSession() as session:
                response = await session.get(url)
                if response.status != 200: return None
                content = await response.text()
                return content

    async def __download_img(self, url, filename):
        content = await self.__get_content(url)
        if content is not None:
            save_path = os.path.join(self.save_dir, filename)
            with open(save_path, "w") as f:
                f.write(content)

    def __call__(self, urls):
        tasks = [asyncio.ensure_future(self.__download_img(img.url, img.get_filename())) for i, img in enumerate(urls)]
        asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))


def download_images(filename, save_dir="./"):
    urls = load_urls(filename)
    print("urls: {}".format(len(urls)))
    Spider(save_dir)(urls)

    #parallel_download(urls, save_dir)



if __name__ == "__main__":
    download_images(sys.argv[1], sys.argv[2])
