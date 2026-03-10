/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: nested:1, settings: { api: { endpoints: { primary: 'https://api.example.com' } } } }]"

[[test.results]]
value = "[{ id: nested:2, settings: { api: { endpoints: { primary: 'https://api.example.com', secondary: 'https://backup.example.com' } } } }]"

[[test.results]]
error = "Couldn't coerce value for field `settings` of `nested:3`: Expected `{ api: { endpoints: { primary: string, secondary: none | string } } }` but found `{ api: { endpoints: { extra: 'key', primary: 'https://api.example.com' } } }`"

[[test.results]]
error = "Couldn't coerce value for field `settings` of `nested:4`: Expected `{ api: { endpoints: { primary: string, secondary: none | string } } }` but found `{ api: { endpoints: { secondary: 'https://backup.example.com' } } }`"

*/
define field settings on nested type {
    api: {
        endpoints: {
            primary: string,
            secondary: option<string>
        }
    }
};

create nested:1 set settings = { api: { endpoints: { primary: "https://api.example.com" } } };
create nested:2 set settings = { api: { endpoints: { primary: "https://api.example.com", secondary: "https://backup.example.com" } } };
create nested:3 set settings = { api: { endpoints: { primary: "https://api.example.com", extra: "key" } } }; // Extra key in nested
create nested:4 set settings = { api: { endpoints: { secondary: "https://backup.example.com" } } }; // Missing required 'primary'

