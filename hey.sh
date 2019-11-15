ulimit -n 10000

hey \
-n 10000 \
-c 100 \
-q 100 \
-m POST \
-D hey.sql \
-H "Content-Type: application/json" \
-H "Bearer: eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJEQiI6Indpc3F1ZSIsIklEIjoiYmJjZmozYXI1azhnMDBiNnZldGciLCJJUCI6Ijg0LjY0LjU2LjExMSIsIk5TIjoid2lzcXVlIiwiU0MiOiJ1c2VyIiwiVEIiOiJ1c2VyIiwiVEsiOiJkZWZhdWx0IiwiZXhwIjoxNTc0MzgxNDA2LCJpYXQiOjE1NzM3NzY2MDYsImlzcyI6IlN1cnJlYWwiLCJuYmYiOjE1NzM3NzY2MDZ9.qs6rOxgy6qxQnS1I1Z1GDPH86WW17xbSFw7nXXC6Dosmy78Lb34tr6w0dXnLZj4Yos7wfpbjw13jSPwA6LGezA" \
http://127.0.0.1:8000/sql
