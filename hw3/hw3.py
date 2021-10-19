import psycopg2

connection = psycopg2.connect("dbname=cs143 user=root password=cs143Rocks! host=localhost")
cur = connection.cursor()

query = '''
select user_id, sum(trip_cost)::decimal(10,2) as total_spend
from (
select user_id, trip_id, trip_length, (1 + trip_length * 0.15)::decimal(10,2) as trip_cost
    from (
        select ts.user_id, ts.trip_id,
        case
        when te.time is null then 1440
        else ceiling((extract(epoch from te.time) - extract(epoch from ts.time))::decimal / 60)
        end trip_length
        from trip_start ts
        left join trip_end te on ts.trip_id = te.trip_id
    ) trip_time
) a
group by user_id
order by user_id
'''

cur.execute(query)

rows = cur.fetchall()

print("BIRD SCOOTER")
print("User Charges for 2021\n")
print("User ID",'\t',"Charge")
print('-'*11,'-'*11,sep='\t')

for user_id, charge in rows:
	print(user_id, f'$ {charge}',sep='\t'*2)

connection.close()