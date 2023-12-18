import uuid

def main(event, context):
    print('The job is submitted successfully!')
    # Return the handling result
    return {
        "id": event.get('id', str(uuid.uuid4())),
        "status": "SUCCEEDED",
    }