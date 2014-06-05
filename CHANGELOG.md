# Tideland Go Data Management

## 2014-06-05

- Added pipelining to version 3 of the Redis client

## 2014-05-31

- Added version 3 of the Redis client
    - Fixed several smaller design errors regarding the result set
    - Added some convenience methods to the result set
    - Changed the concept of retrieving and returning a connection
    - Removed the extra handling of transactions, simply use
      the commands
    - Removed the useless asynchronous command
    - Simplified the internal design for better maintenance

## 2014-04-25

- Moved the repository to `github.com`
- Added major version numbers to the import path

