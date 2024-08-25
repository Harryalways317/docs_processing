from PyPDF2 import PdfReader, PdfWriter, PageObject
from PyPDF2.generic import RectangleObject
import os
def split_side_by_side_pages(input_pdf_path, output_pdf_path):
    # Open the input PDF
    input_pdf = PdfReader(input_pdf_path)
    output_pdf = PdfWriter()

    for page_num in range(len(input_pdf.pages)):
        print("page_num", page_num)
        page = input_pdf.pages[page_num]
        width = page.mediabox.upper_right[0]
        height = page.mediabox.upper_right[1]

        # Define A4 size in points (1 inch = 72 points, A4 is 8.27 x 11.69 inches)
        A4_WIDTH = 595.44
        A4_HEIGHT = 841.68

        # Check if the page needs to be split
        if width > height and width > A4_WIDTH:
            # Split the page vertically into two new pages
            middle_x = width / 2

            # Create two new pages with the split content
            left_page = PageObject.create_blank_page(width=middle_x, height=height)
            right_page = PageObject.create_blank_page(width=middle_x, height=height)

            # Copy the content of the left half
            left_crop = RectangleObject([0, 0, middle_x, height])
            left_page.merge_page(page)
            left_page.mediabox = left_crop

            # Copy the content of the right half
            right_crop = RectangleObject([middle_x, 0, width, height])
            right_page.merge_page(page)
            right_page.mediabox = right_crop

            # Add the new pages to the output PDF
            output_pdf.add_page(left_page)
            output_pdf.add_page(right_page)
        else:
            # If the page does not need to be split, just add it to the output PDF
            output_pdf.add_page(page)

    # Save the output PDF
    with open(output_pdf_path, 'wb') as output_file:
        output_pdf.write(output_file)

# Example usage
def process_pdfs_in_directory(directory):
    for filename in os.listdir(directory):
        print(filename)
        if filename.endswith('SPLIT.BRSR.pdf'):
            input_pdf_path = os.path.join(directory, filename)
            output_pdf_path = os.path.join(directory, filename.replace('.SPLIT.BRSR.pdf', '.BRSR.pdf'))
            split_side_by_side_pages(input_pdf_path, output_pdf_path)
            print(f"Processed: {input_pdf_path} -> {output_pdf_path}")
            os.remove(input_pdf_path)  # Delete the original file
            print(f"Deleted original file: {input_pdf_path}")


# Define the directory
directory = 'BRSR'

# Process all matching PDFs in the directory
process_pdfs_in_directory(directory)
print("All matching files processed.")
