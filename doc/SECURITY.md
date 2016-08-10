# Surreal

Surreal is a NoSQL Document and Graph database

---

### Authentication

- Accept connection on HTTP (RESTful)
- Check JWT token
    - Get id from token *(account id)*
    - Get db from token *(database name)*
    - Check token against database `/surreal/{id}/{db}/!/tokens/{token}`
    - `HTTP 403` if token does not exist


- Accept connection on HTTP (Websocket)
- Check JWT token
    - Get id from token *(account id)*
    - Get db from token *(database name)*
    - Check token against database `/surreal/{id}/{db}/!/tokens/{token}`
    - `HTTP 403` if token does not exist