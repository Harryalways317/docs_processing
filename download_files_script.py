# # import pandas as pd
# # import requests
# # import os
# #
# #
# # def download_and_rename(csv_file, output_folder):
# #     # Load the CSV file
# #     df = pd.read_csv(csv_file)
# #
# #     # Create the output folder if it does not exist
# #     os.makedirs(output_folder, exist_ok=True)
# #
# #     # Open the status file in append mode
# #     with open(os.path.join(output_folder, 'status.txt'), 'a') as status_file:
# #         # Iterate over each row in the DataFrame
# #         for index, row in df.iterrows():
# #             ticker = row['Symbol']
# #             print("ticker ",ticker)
# #
# #             ar_url = row['AR for FY 24']
# #             print("URL", ar_url)
# #             output_filename = f"{ticker}.AR.pdf"
# #             output_path = os.path.join(output_folder, output_filename)
# #
# #             # Log the processing status
# #             status_file.write(f"Processing {ticker}\n")
# #             status_file.flush()
# #
# #             # Download the file
# #             try:
# #                 # response = requests.get(ar_url)
# #                 headers = {
# #                     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
# #                     'Referer': 'https://nsearchives.nseindia.com'
# #                 }
# #
# #                 response = requests.get(ar_url, headers=headers)
# #                 print(response)
# #                 print(response.status_code)
# #                 response.raise_for_status()  # Raise an error on a bad status
# #                 # Write the content to a new file
# #                 with open(output_path, 'wb') as f:
# #                     f.write(response.content)
# #                 # Log the completed status
# #                 status_file.write(f"Completed {ticker}\n")
# #                 status_file.flush()
# #             except requests.RequestException as e:
# #                 # Log any errors
# #                 print("Error",e)
# #                 status_file.write(f"Failed {ticker}: {e}\n")
# #                 status_file.flush()
# #
# #
# # # Usage
# # csv_file_path = 'companies.csv'
# # output_folder = 'downloaded_reports'
# # download_and_rename(csv_file_path, output_folder)
#
#
# import pandas as pd
# import requests
# import os
# from concurrent.futures import ThreadPoolExecutor
# from PyPDF2 import PdfReader, PdfWriter
# from PyPDF2.generic import RectangleObject, PageObject
#
#
# def split_side_by_side_pages(input_pdf_path, output_pdf_path):
#     input_pdf = PdfReader(input_pdf_path)
#     output_pdf = PdfWriter()
#
#     for page_num in range(len(input_pdf.pages)):
#         page = input_pdf.pages[page_num]
#         width = page.mediabox.upper_right[0]
#         height = page.mediabox.upper_right[1]
#
#         A4_WIDTH = 595.44
#
#         if width > height and width > A4_WIDTH:
#             middle_x = width / 2
#             left_page = PageObject.create_blank_page(width=middle_x, height=height)
#             right_page = PageObject.create_blank_page(width=middle_x, height=height)
#
#             left_crop = RectangleObject([0, 0, middle_x, height])
#             left_page.merge_page(page)
#             left_page.mediabox = left_crop
#
#             right_crop = RectangleObject([middle_x, 0, width, height])
#             right_page.merge_page(page)
#             right_page.mediabox = right_crop
#
#             output_pdf.add_page(left_page)
#             output_pdf.add_page(right_page)
#         else:
#             output_pdf.add_page(page)
#
#     with open(output_pdf_path, 'wb') as output_file:
#         output_pdf.write(output_file)
#
#
# def download_file(ticker, ar_url, output_path, status_file):
#     # Decide headers based on URL
#     if 'bse' in ar_url:
#         headers = {
#             'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
#             'Referer': 'https://www.bseindia.com',
#             'If-Modified-Since': 'Sat, 24 Aug 2024 17:30:52 GMT',
#             'If-None-Match': '"c0b5665e4bf6da1:0"'
#         }
#     else:
#         headers = {
#             'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
#             'Referer': 'https://nsearchives.nseindia.com'
#         }
#
#     try:
#         response = requests.get(ar_url, headers=headers, stream=True)
#         response.raise_for_status()
#
#         temp_path = output_path.replace(".pdf", "_temp.pdf")
#         with open(temp_path, 'wb') as f:
#             for chunk in response.iter_content(chunk_size=8192):
#                 f.write(chunk)
#
#         # Check if the PDF needs to be split
#         input_pdf = PdfReader(temp_path)
#         page = input_pdf.pages[0]
#         width = page.mediabox.upper_right[0]
#         height = page.mediabox.upper_right[1]
#
#         A4_WIDTH = 595.44
#         final_path = output_path
#
#         if width > height and width > A4_WIDTH:
#             split_path = output_path.replace(".pdf", ".SPLIT.AR.pdf")
#             split_side_by_side_pages(temp_path, split_path)
#             final_path = split_path
#
#         os.rename(temp_path, final_path)
#         status_file.write(f"Completed {ticker}\n")
#     except requests.RequestException as e:
#         status_file.write(f"Failed {ticker}: {e}\n")
#
#
# def download_and_rename(csv_file, output_folder):
#     df = pd.read_csv(csv_file)
#     os.makedirs(output_folder, exist_ok=True)
#     status_file_path = os.path.join(output_folder, 'status.txt')
#
#     with ThreadPoolExecutor(max_workers=10) as executor, open(status_file_path, 'a') as status_file:
#         futures = []
#         for index, row in df.iterrows():
#             ticker = row['Symbol']
#             ar_url = row['AR for FY 24']
#             output_filename = f"{ticker}.AR.pdf"
#             output_path = os.path.join(output_folder, output_filename)
#             status_file.write(f"Processing {ticker}\n")
#             status_file.flush()
#             future = executor.submit(download_file, ticker, ar_url, output_path, status_file)
#             futures.append(future)
#
#         for future in futures:
#             future.result()
#
#
# # Usage
# csv_file_path = 'companies.csv'
# output_folder = 'downloaded_reports'
# download_and_rename(csv_file_path, output_folder)


import pandas as pd
import requests
import os
from concurrent.futures import ThreadPoolExecutor
from PyPDF2 import PdfReader, PdfWriter,PageObject
from PyPDF2.generic import RectangleObject

def split_side_by_side_pages(input_pdf_path, output_pdf_path):
    input_pdf = PdfReader(input_pdf_path)
    output_pdf = PdfWriter()

    for page_num in range(len(input_pdf.pages)):
        page = input_pdf.pages[page_num]
        width = page.mediabox.upper_right[0]
        height = page.mediabox.upper_right[1]

        A4_WIDTH = 595.44  # Standard A4 width in points

        if width > height and width > A4_WIDTH * 1.5:  # Check if the page is significantly wider than A4
            middle_x = width / 2
            left_page = PageObject.create_blank_page(width=middle_x, height=height)
            right_page = PageObject.create_blank_page(width=middle_x, height=height)

            # Crop and add left half
            left_crop = RectangleObject([0, 0, middle_x, height])
            left_page.merge_page(page)
            left_page.mediabox = left_crop

            # Crop and add right half
            right_crop = RectangleObject([middle_x, 0, width, height])
            right_page.merge_page(page)
            right_page.mediabox = right_crop

            output_pdf.add_page(left_page)
            output_pdf.add_page(right_page)
        else:
            output_pdf.add_page(page)

    with open(output_pdf_path, 'wb') as output_file:
        output_pdf.write(output_file)

def download_file(ticker, ar_url, output_path, status_file):
    if 'bse' in ar_url:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'Referer': 'https://www.bseindia.com',
            'If-Modified-Since': 'Sat, 24 Aug 2024 17:30:52 GMT',
            'If-None-Match': '"c0b5665e4bf6da1:0"'
        }
    else:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'Referer': 'https://nsearchives.nseindia.com'
        }

    try:
        response = requests.get(ar_url, headers=headers, stream=True)
        response.raise_for_status()

        temp_path = output_path.replace(".pdf", "_temp.pdf")
        with open(temp_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        input_pdf = PdfReader(temp_path)
        page = input_pdf.pages[0]
        width = page.mediabox.upper_right[0]
        height = page.mediabox.upper_right[1]
        A4_WIDTH = 595.44

        if width > height and width > A4_WIDTH * 1.5:
            print("Needed Split",ticker)
            split_path = output_path.replace(".pdf", "_SPLIT.pdf")
            split_side_by_side_pages(temp_path, split_path)
            os.rename(split_path, output_path)  # Rename split file back to the original format
            print("Split Done", ticker)
        else:
            os.rename(temp_path, output_path)

        status_file.write(f"Completed {ticker}\n")
    except requests.RequestException as e:
        status_file.write(f"Failed {ticker}: {e}\n")

def download_and_rename(csv_file, output_folder):
    df = pd.read_csv(csv_file)
    os.makedirs(output_folder, exist_ok=True)
    status_file_path = os.path.join(output_folder, 'status.txt')

    with ThreadPoolExecutor(max_workers=4) as executor, open(status_file_path, 'a') as status_file:
        futures = []
        for index, row in df.iterrows():
            ticker = row['Symbol']
            print("ticker ",ticker)
            ar_url = row['AR for FY 24']
            print("ar_url ", ticker)
            output_filename = f"{ticker}.AR.pdf"
            output_path = os.path.join(output_folder, output_filename)
            if pd.isna(ar_url) or ar_url.strip().lower() == 'na':
                status_file.write(f"Skipping {ticker}, URL is NA.\n")
                continue
            status_file.write(f"Processing {ticker}\n")
            status_file.flush()
            future = executor.submit(download_file, ticker, ar_url, output_path, status_file)
            futures.append(future)

        for future in futures:
            future.result()

# Usage
csv_file_path = 'companies.csv'
output_folder = 'downloaded_reports'
download_and_rename(csv_file_path, output_folder)
