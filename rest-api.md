# REST API

This is the specification of the IMC REST API. 

## Infrastructure API

The Infrastructure API exposes endpoints for managing infrastructure. Note that the API is asynchronous, i.e. it immediately gives a response but the action is carried out in the background.

### Describe infrastructure

```
GET /v1/infrastructures/<id>
```

Get information about the specified infrastructure: current status, cloud name (where applicable), Infrastructure Manager id.

#### Example Request

```http
GET /v1/infrastructures/b4486ddb-0bb5-4056-b74a-5adf49928eb6 HTTP/1.1
```

#### Example Response

```http
HTTP/1.1 200 OK
Content-Type: application/json
```

```json
{
  "status": "configured",
  "cloud": "MyOpenStack",
  "infra_id": "e1f8e7e3-a250-4bd5-9ab1-7cb719ad886f"
}
```

### Query Parameters

None.

#### Status Codes

- **200** - no error
- **404** - not found

### Deploy infrastructure

```
POST /v1/infrastructures
Content-Type: application/json
```
Deploy infrastructure.

#### Example Request

#### Example Response

```http
HTTP/1.1 201 Created
Content-Type: application/json
```

```json
{
  "id": "b4486ddb-0bb5-4056-b74a-5adf49928eb6"
}
```
#### Request Body

The following fields are used in the request body for creating a job:



#### Status Codes

- **201** - no error
- **400** - bad request

### Delete infrastructure

```
DELETE /v1/infrastructures/<id>
```

Delete the specified infrastructure.

#### Example Request

```http
DELETE /v1/infrastructures/b4486ddb-0bb5-4056-b74a-5adf49928eb6 HTTP/1.1
```

#### Example Response

#### Status Codes

- **200** - no error

