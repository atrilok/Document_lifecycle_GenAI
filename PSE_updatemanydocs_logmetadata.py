from astrapy import DataAPIClient
from datetime import datetime
import uuid

# Initialize the client
client = DataAPIClient("AstraCS:IZOGLgZALusboxhhaegqJymg:333d7002876e83c4a46d2e058ca0087ece1d0f55cc6bc8e848f7c925b228e513")
db = client.get_database_by_api_endpoint(
  "https://f44739af-e770-4360-8001-7be7c2f0ac86-us-east-2.apps.astra.datastax.com"
)

print(f"Connected to Astra DB: {db.list_collection_names()}")

def current_timestamp():
    return datetime.now().isoformat()


def generate_unique_identifier():
    return str(uuid.uuid4())


data_collection = db.get_collection("original_documentlifecycle_data")
metadata_collection = db.get_collection("change_documentlifecycle")

Updated_collection = db.get_collection("updated_documentlifecycle_data")

def update_multiple_documents(content_value, update_fields):

    query = {"content": content_value}

    # Fetch original documents and convert cursor to list
    original_docs = list(data_collection.find(query))
    print("Original docs fetched:", len(original_docs))

    # Perform bulk updates
    data_collection.update_many(query, {"$set": update_fields})

    # Re-fetch the updated documents to log the changes, using a query that reflects the updates
    updated_query = {key: update_fields[key] for key in update_fields}
    updated_docs = list(data_collection.find(updated_query))
    print("Updated docs fetched:", len(updated_docs))

    # Log the changes
    log_changes(original_docs, updated_docs, metadata_collection)

def log_changes(original_docs, updated_docs, metadata_collection):

    if not original_docs or not updated_docs:
        print("Error: One or both document lists are empty.")
        return

    for original, updated in zip(original_docs, updated_docs):
        change_description = {}
        for key in update_fields.keys():
            original_value = original.get(key)
            updated_value = updated.get(key)
            if original_value != updated_value:
                change_description[key] = {"old": original_value, "new": updated_value}

        if change_description:
            metadata_entry = {
                "change_id": generate_unique_identifier(),
                "original_document_id": original.get('document_id'),
                "new_document_id": updated.get('document_id'),
                "timestamp": current_timestamp(),
                "change_description": change_description
            }
            metadata_collection.insert_one(metadata_entry)
            print("Change logged:", metadata_entry)
        else:
            print("No changes detected for document ID", original.get('_id'))

# Example parameters
update_fields = {
    "content": "PSE has new initiatives for electric car owners"
}
content_value = "Puget Sound's new electric vehicle charging stations initiative"

update_multiple_documents(content_value, update_fields)

