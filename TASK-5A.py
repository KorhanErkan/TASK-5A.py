from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.orm import sessionmaker
from faker import Faker
import time
import random
import pandas as pd
start_time=time.time()

engine = create_engine("postgresql://postgres:2511@localhost:5432/postgres")
dbConnection = engine.connect()
Base = declarative_base()
Base.metadata.bind = engine
session = sessionmaker(bind=engine)()
fake = Faker()


def Generator(i):
  L1 = []
  for i in range(i):
   L2=[]
   L2.append(fake.email())
   L2.append(fake.password())
   L1.append(L2)
  return L1

df1 = pd.DataFrame(Generator(10000), columns=["email", "sifre"])

class login(Base):
    __tablename__ = "login"
    __table_args__ = {"extend_existing": True}
    email3 = Column(String, primary_key=True)
    sifre3 = Column(String(20))
Base.metadata.create_all(engine)
session.commit()

df2 = pd.read_sql("select * from \"login\"", dbConnection);

df2.insert(0, "email", df1['email'])
df2.insert(1, "sifre", df1["sifre"])
del df2["email3"]
del df2["sifre3"]

df2.to_sql('login2', engine, if_exists='replace')           #ilk 10 k
sample2 = df2.sample(n = 1000, replace = False)
df3 = pd.DataFrame(sample2, columns=["email", "sifre"])
df4 = pd.DataFrame(Generator(9000), columns=["email", "sifre"])
df5 = df3.append(df4, ignore_index=True)                    #1000 aynı 9000 farklı
ind = df5.email.isin(df2.email) & df5.sifre.isin(df2.sifre)
a = ([i for i, x in enumerate(ind) if x])
df6 = df5.iloc[0:len(a)]
df6.to_sql('findings', engine, if_exists='replace')
end_time=time.time()
print(end_time-start_time)
dbConnection.close();

# Time = 3.12953519821167