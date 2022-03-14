import urllib.request
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
import time
import os

base_url = ''  # Removed for confidentiality reasons
print(base_url)
driver = webdriver.Firefox()
driver.get(base_url)
time.sleep(15)
albums = driver.find_elements_by_class_name(
    "media-box-thumbnail-container")

for a in range(20, len(albums)):
    if a == 0:  # Kit 1 only has 1 layer
        try:
            albums[a].click()
        except StaleElementReferenceException:
            albums = driver.find_elements_by_class_name(
                "media-box-thumbnail-container")
            albums[a].click()
        time.sleep(10)
        photos = driver.find_elements_by_class_name('media-box-image')
        for p in photos:
            soup = BeautifulSoup(p.get_attribute('outerHTML'))
            img_url = base_url + soup.div['data-src']
            filename = img_url.split('/')[-1]
            dir = img_url.split('/')[-2]
            img_url = img_url.replace(" ", "%20")
            try:
                os.mkdir(dir)
            except Exception:
                pass
            urllib.request.urlretrieve(img_url, f"{dir}/{filename}")
        # Backtracking
        layers = driver.find_elements_by_tag_name('li')
        layers[0].click()
        time.sleep(10)
    else:
        albums = driver.find_elements_by_class_name(
            "media-box-thumbnail-container")
        albums[a].click()
        time.sleep(40)
        pieces = driver.find_elements_by_class_name('media-box-is-directory')
        for p in range(len(pieces)):
            pieces = driver.find_elements_by_class_name(
                'media-box-is-directory')
            pieces[p].click()
            time.sleep(15)
            photos = driver.find_elements_by_class_name('media-box-image')
            for ph in range(len(photos)):
                photos = driver.find_elements_by_class_name('media-box-image')
                soup = BeautifulSoup(photos[ph].get_attribute('outerHTML'))
                img_url = base_url + soup.div['data-src']
                filename = img_url.split('/')[-1]
                out_dir = img_url.split('/')[-3]
                in_dir = img_url.split('/')[-2]
                img_url = img_url.replace(" ", "%20")
                try:
                    os.mkdir(out_dir)
                except Exception:
                    pass
                try:
                    os.mkdir(out_dir + '/' + in_dir)
                except Exception:
                    pass
                urllib.request.urlretrieve(
                    img_url, f"{out_dir}/{in_dir}/{filename}")
            # Backtracking
            layers = driver.find_elements_by_tag_name('li')
            layers[1].click()
            time.sleep(40)
        layers = driver.find_elements_by_tag_name('li')
        layers[0].click()
        time.sleep(40)

driver.quit()
# with open('acrdb.txt', 'w') as f:
#     for item in img_url_arr:
#         f.write("%s\n" % item)
