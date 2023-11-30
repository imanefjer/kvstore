# HTTP Key-Value Store Readme

## Introduction

This project implements a simple HTTP-based persistent key-value store in Go. It leverages concepts such as LSM trees, Write-Ahead Logging (WAL), and periodic flushing to disk to ensure data integrity and crash safety.

## Project Structure

### 1. **Main Application**

The main application file (`main.go`) sets up the HTTP server, initializes the Write-Ahead Log (WAL), and creates a new instance of the key-value store database. It defines three HTTP endpoints for GET, SET, and DEL operations.

### 2. **Handlers**

- **GET Handler (`GetHandler`):**
  - Processes GET requests to retrieve the value associated with a given key.
  - Checks the in-memory tree (binary search tree) and SSTables for the key.
  - Responds with the value or an error if the key is not found.

- **SET Handler (`SetHandler`):**
  - Processes POST requests to set a key-value pair.
  - Validates the request payload (JSON format) for key and value.
  - Sets the value in the in-memory tree.
  - Appends the operation to the Write-Ahead Log (WAL) for crash safety.
  - Flushes the in-memory tree to disk if the maximum capacity is reached.
  - Initiates compaction if the number of SST files reaches the maximum limit.


- **DEL Handler (`DelHandler`):**
  - Processes DELETE requests to delete a key.
  - Checks if the key exists in the in-memory tree.
  - If not, searches for it in the SSTables.
  - Marks the key as deleted in the in-memory tree or adds it as a deleted key if found in SSTables.
  - Appends the deletion command to the Write-Ahead Log (WAL).
  - Flushes the in-memory tree to disk if the maximum capacity is reached.
  - Initiates compaction if the number of SST files reaches the maximum limit

- **Default Handler (`DefaultHandler`):**
  - Handles unknown commands with a default response.

### 3. **FlushToDisk Function**

The `FlushToDisk` function:
- Flushes the in-memory tree to disk.
- Reinitializes the tree.
- Adds a watermark to the Write-Ahead Log (WAL) for tracking.

### 4. **Project Configuration**

- Defines a maximum capacity (`max`) for the in-memory tree before flushing to disk.
- Initializes a Write-Ahead Log (WAL) and sets up an HTTP server on port 8084.

## Running the Application

1. Clone the repository.
2. Run the application with `go run main.go`.
3. Access the key-value store endpoints:
   - GET: `http://localhost:8084/get?key=keyName`
   - SET: `http://localhost:8084/set` (POST with JSON payload)
   - DEL: `http://localhost:8084/del?key=keyName`

## Testing

To test the key-value store, you can use tools like `curl` or Postman. Here are some sample requests:

#### GET

Retrieve the value associated with a key:

```bash
curl http://localhost:8084/get?key=keyName
```
#### SET
Set a key-value pair using a POST request with JSON payload:

```bash
curl -X POST -H "Content-Type: application/json" -d '{"key": "exampleKey", "value": "exampleValue"}' http://localhost:8084/set
```
#### DEL
Delete a key from the database:

```bash
curl -X DELETE http://localhost:8084/del?key=keyName
```
