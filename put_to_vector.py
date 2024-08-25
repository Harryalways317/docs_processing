import json
from openai import AzureOpenAI
import weaviate



import weaviate

vector_db = weaviate.Client("http://52.249.216.32:8081")
embeddings_client = AzureOpenAI(
    azure_endpoint="https://openai-service-onfi.openai.azure.com/",
    api_version="2023-05-15",
    api_key="add2ae8844844d55bd3e1300ccbc9bc2"
)


def generate_vector(query):
    vector = embeddings_client.embeddings.create(input=[query], model="onfi-embedding-model").data[0].embedding
    return vector

def write_to_weaviate(class_name,customer_name, data_list):
    client = weaviate.Client("http://52.249.216.32:8081")
    client.batch.configure(batch_size=100)

    print("Processing data...")

    with client.batch as batch:
        for data in data_list:
            # Combine headers if they exist, separate by a dash if both are present
            header = ""
            if "Header 1" in data["metadata"] and "Header 2" in data["metadata"]:
                header = f"{data['metadata']['Header 1']} - {data['metadata']['Header 2']}"
            elif "Header 1" in data["metadata"]:
                header = data["metadata"]["Header 1"]
            elif "Header 2" in data["metadata"]:
                header = data["metadata"]["Header 2"]

            properties = {
                "content": data["content"],
                "customer_name" : customer_name,
                "is_table": data['metadata'].get("is_table", False),
                "pdf_chunk_number": data['metadata'].get("pdf_chunk_number", 0),
                "page_number": data["metadata"].get("page_number", [None])[0],
                "header": header  # This field will include combined or single headers if available
            }

            print(properties)

            header_content = header + "  " + data["content"]

            # Assuming you have an appropriate vector for this object; you might need to generate or update this vector based on your setup.
            vector = generate_vector(header_content)  # Placeholder for the actual vector

            batch.add_data_object(
                data_object=properties,
                class_name=class_name,
                vector=vector
            )
            print("Added data to", class_name)

    print("Batch import completed for class:", class_name)


class_name = "NSE_GOOD_TEST_003"
file_name = "CHALET.BRSR.json"
customer_name = "CHALET"
with open(file_name, 'r') as file:
    data_list = json.load(file)

write_to_weaviate(class_name,customer_name,data_list)