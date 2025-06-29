f = open("9.txt")

k = 0

for i in f.readlines():
    st = list(map(int, i.strip().split()))
    povt = []
    nepovt = []
    for j in st:
        if st.count(j) > 1:
            povt.append(j)
        else:
            nepovt.append(j)
    if len(povt) == 3 and nepovt[0] * nepovt[1] > sum(povt):
        k += 1
print(k)
