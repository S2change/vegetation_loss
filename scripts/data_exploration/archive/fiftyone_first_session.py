import fiftyone as fo
import fiftyone.zoo as foz
from PIL import Image
import webbrowser

# Update this path to where Chrome is actually installed on your PC
# Common Windows path:C:/Program Files/Google/Chrome/Application
chrome_path = r"C:\Program Files\Google\Chrome\Application\new_chrome.exe %s"

webbrowser.register('chrome', None, webbrowser.BackgroundBrowser(chrome_path))

dataset=foz.load_zoo_dataset("quickstart")

# visualize the first sample in the dataset
session = fo.launch_app(dataset, browser="chrome")





