import xml.etree.ElementTree as ET

import time
from selenium import webdriver
from selenium.webdriver.common.by import By

# Initialize the WebDriver (e.g., Chrome)
# webdriver = webdriver.Chrome(r"C:\Users\NETRAVATI_MADANKAR\Downloads\chromedriver.exe")
# webdriver= webdriver.Chrome(r"chromedriver.exe")
webdriver= webdriver.Chrome(r"C:\Users\NETRAVATI_MADANKAR\Downloads\chromedriver_win32 (2)\chromedriver.exe")
webdriver.maximize_window()
# Navigate to the website
url = "https://pipeline2.kindermorgan.com/TransReports/Transactional.aspx?type=Firm&code=WIC"  # URL

webdriver.get(url)
time.sleep(8)


def extract_product_data():
    products = []
    TSP = ''
    PipelineName = ''
    RawPipelineReportDate = ''
    Post_Time = ''
    DUNS = ''
    RawShipperName = ''
    PipelineServiceCodeGas = ''
    ContractTermStartDate = ''
    ContractTermEndDate = ''
    for i in webdriver.find_elements(By.XPATH,
                                     '//*[@id="WebSplitter1_tmpl1_ContentPlaceHolder1_DGTransDetail"]/table/tbody/tr[1]/td[1]/table/tbody[2]/tr/td/div[2]/table/tbody/tr[*]/td[2]'):
        # print(i.text)
        # time.sleep(15)
        # webdriver.execute_script("return document.body.scrollHeight")
        # webdriver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        # time.sleep(8)
        # scroll down table
        # scrol_tabl = webdriver.find_element(By.XPATH,
        #                                     '//*[@id="WebSplitter1_tmpl1_ContentPlaceHolder1_DGTransDetail"]/table/tbody/tr[1]/td[2]/div')
        # webdriver.execute_script("arguments[0].scrollTop+=1000;", scrol_tabl)
        # time.sleep(6)

        import re
        data = i.text

        data_list = data.split("\n")

        for line in data_list:
            print(line)
            print("------------------")
            if "TSP" in line:
                pattern = r'\b\d{8}\b'
                matches = re.findall(pattern, line)
                if matches:
                    TSP = matches[0]
                    # print("tsp number", TSP)
            if "TSP Name" in line:
                PipelineName = line.split(":")[-1].strip()
                # print("tsp name", PipelineName)
            if "Post Date" in line:
                RawPipelineReportDate = line.split(":")[1].strip().replace("Post Time", "")
                # print("post date: ", RawPipelineReportDate)
            if "Post Time" in line:
                Post_Time = ":".join(line.split(":")[2:])
                # print("post_time", post_time)

            if "K Holder" in line:
                pattern = r'\b\d{9}\b'
                matches = re.findall(pattern, line)
                if matches:
                    DUNS = matches[0]
                    # print("kh number", DUNS)
            if "K Holder Name" in line:
                RawShipperName = line.split(":")[-1].strip()
                # print("kh name", RawShipperName)
            if "Rate Sch" in line:
                PipelineServiceCodeGas = line.split(":")[1]
                # print("PipelineServiceCodeGas",PipelineServiceCodeGas)

            if "K Beg Date" in line:
                ContractTermStartDate = line.split(":")[1]
                # print("ContractTermStartDate",ContractTermStartDate)
            if "K End Date" in line:
                ContractTermEndDate = line.split(":")[1]
                # print("ContractTermEndDate",ContractTermEndDate)

    product_data_dict = {"TSP": TSP, "PipelineName": PipelineName, "RawPipelineReportDate": RawPipelineReportDate,
                    "Post_Time": Post_Time, "DUNS": DUNS, "RawShipperName": RawShipperName,
                    "PipelineServiceCodeGas": PipelineServiceCodeGas,
                    "ContractTermStartDate": ContractTermStartDate, "ContractTermEndDate": ContractTermEndDate}
    products.append(product_data_dict)
    print("product_list", products)
    return products


for index_ in range(5):

    # Looping over rows
    print("Fetching file: {}".format(index_))
    webdriver.find_element(By.XPATH,
                           "//*[@id='ctl00_WebSplitter1_tmpl1_ContentPlaceHolder1_DGTransactional_it0_{}_lnkbtnDetails']".format(
                               index_)).click()  # View button click
    time.sleep(5)

    products_data = extract_product_data()
    root = ET.Element("TransactionalReporting")

    for product_data in products_data:
        parent = ET.SubElement(root, "TransactionalReportingPipelineRaw")

        PipelineName = ET.SubElement(parent, "PipelineName")
        PipelineName.text = product_data["PipelineName"]

        RawPipelineReportDate = ET.SubElement(parent, "RawPipelineReportDate")
        RawPipelineReportDate.text = product_data["RawPipelineReportDate"]

        RawPipelineMeasurementStorage = ET.SubElement(parent, "RawPipelineMeasurementStorage")
        RawPipelineMeasurementStorage.text = 'Dth'

        RawPipelineMeasurementTrans = ET.SubElement(parent, "RawPipelineMeasurementTrans")
        RawPipelineMeasurementTrans.text = 'Dth'

        price_element = ET.SubElement(parent, "TSP")
        price_element.text = product_data["TSP"]

        KeyPipelineReportType = ET.SubElement(parent, "KeyPipelineReportType")
        KeyPipelineReportType.text = 'Firm'

        # next tag starts here
        parent_sub = ET.SubElement(root, "TransactionalReportingContractTermsRaw")

        RawShipperName = ET.SubElement(parent_sub, "RawShipperName")
        RawShipperName.text = product_data["RawShipperName"]

        DUNS = ET.SubElement(parent_sub, "DUNS")
        DUNS.text = product_data["DUNS"]

        PipelineServiceCodeGas = ET.SubElement(parent_sub, "PipelineServiceCodeGas")
        PipelineServiceCodeGas.text = product_data["PipelineServiceCodeGas"]

        ContractTermStartDate = ET.SubElement(parent_sub, "ContractTermStartDate")
        ContractTermStartDate.text = product_data["ContractTermStartDate"]

        ContractTermEndDate = ET.SubElement(parent_sub, "ContractTermEndDate")
        ContractTermEndDate.text = product_data["ContractTermEndDate"]

    # Create an ElementTree object
    tree = ET.ElementTree(root)

    # Write the XML structure to a new XML file
    output_file = "wyoming_interstate_xmlfile{}.xml".format(index_)
    tree.write(output_file, encoding="utf-8", xml_declaration=True)

    print(f"Successfully extracted data and stored in {output_file}")
    webdriver.back()
