# Data dictionary

## states table

| Columns    | Data Type | Description   |
| :--------: |:---------:| :------------:|
| state_code | object    | US state code |
| state      | object    | US state name |

## cities table

| Columns  | Data Type | Description |
| :------: |:---------:| :----------:|
| state_id | int64     | state index |
| city     | object    | city name   |


## demographics table

| Columns  | Data Type | Description |
| :------------------------------: |:-----:| :------------------------------------------:|
| city_id                          |int64  | city index                                  |
| median_age                       |float64| median age of people                        |
| female_population                |int64  | female population                           |
| total_population                 |int64  | male population                             |      
| number_of_veterans               |int64  | number of veterans                          |
| foreign_born                     |int64  |foreign-born population                      |
| average_household_size           |float64| average household size                      |
| american_indian_and_alaska_native|int64  | American Indian and Alaska native population|
| asian                            |int64  | Asian population                            |
| black_or_african_american        |int64  | Black or african american population        |
| hispanic_or_latino               |int64  | Hispanic or Latino population               |
| white                            |int64  | Whites population                           |

## airports table

| Columns     | Data Type | Description                             |
| :---------: |:---------:| :--------------------------------------:|
| city_id     | int64     | index of a city                         |
| ident       | object    | identification number of an airport     |
| airport_name| object    | airport name                            |
| type        | object    | airport type                            |
| elevation_ft| int64     | highest point on the usable landing area|
| coordinates | object    | airport's coordinates                   |

## flights table

| Columns            | Data Type      | Description       |
| :----------------: |:--------------:| :----------------:|
| city_id            | int64          | city index        |
| origin_country     | object         | departure country |
| arrival_date_usa   | datetime64[ns] | arrival city      |
| departure_date_usa | datetime64[ns] | departure date    |
| mode               | object         | air or sea        |
| arrival_flag       | object         | departure flag    |
| departure_flag     | object         | arrival flag      |
| airline            | object         | airline code      |
| flight_number      | object         | flight's number   |

## passengers table

| Columns                  | Data Type      | Description                       |
| :----------------------: |:--------------:| :--------------------------------:|
| flight_id                | int64          | flight index                      |
| passenger_number         | int64          | passenger's identification number |
| gender                   | object         | M - male F - female               |          
| birth_year               | int64          | birth year                        |
| passenger_age            | int64          | passenger's age                   |
| visa_type                | object         | visa type                         |
| visa_code                | int64          | visa code                         |
| admission_number         | int64          | admission number                  |
| date_until_allowed_stay  | datetime64[ns] | date until allowed stay in USA    |