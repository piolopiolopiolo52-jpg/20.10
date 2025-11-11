db.employees.drop()
db.departments.drop()

db.employees.insertMany([
  { _id: 1, name: "Олег", department_id: 10, salary: 95000 },
  { _id: 2, name: "Анна", department_id: 10, salary: 105000 },
  { _id: 3, name: "Дмитрий", department_id: 11, salary: 88000 }
])

db.departments.insertMany([
  { _id: 10, name: "Маркетинг" },
  { _id: 11, name: "Продажи" }
])

print("\n1️⃣ Сотрудник и его отдел:")
db.employees.aggregate([
  {
    $lookup: {
      from: "departments",
      localField: "department_id",
      foreignField: "_id",
      as: "department"
    }
  },
  { $unwind: "$department" },
  {
    $project: {
      _id: 0,
      employee: "$name",
      department: "$department.name"
    }
  }
]).forEach(printjson)

print("\n2️⃣ Средняя зарплата по отделам:")
db.employees.aggregate([
  {
    $group: {
      _id: "$department_id",
      avgSalary: { $avg: "$salary" }
    }
  },
  {
    $lookup: {
      from: "departments",
      localField: "_id",
      foreignField: "_id",
      as: "department"
    }
  },
  { $unwind: "$department" },
  {
    $project: {
      _id: 0,
      department: "$department.name",
      avgSalary: 1
    }
  }
]).forEach(printjson)

print("\n3️⃣ Добавляем новый отдел и переводим туда Олега:")
db.departments.insertOne({ _id: 20, name: "Разработка" })
db.employees.updateOne(
  { name: "Олег" },
  { $set: { department_id: 20 } }
)
print("✅ Добавлен отдел 'Разработка' и Олег переведён.")

print("\n4️⃣ Удаляем отделы без сотрудников:")
db.departments.aggregate([
  {
    $lookup: {
      from: "employees",
      localField: "_id",
      foreignField: "department_id",
      as: "emps"
    }
  },
  { $match: { emps: { $size: 0 } } }
]).forEach(dept => {
  db.departments.deleteOne({ _id: dept._id })
  print("❌ Удалён отдел:", dept.name)
})

print("\n5️⃣ Сотрудники с зарплатой выше средней по их отделу:")
db.employees.aggregate([
  {
    $group: {
      _id: "$department_id",
      avgSalary: { $avg: "$salary" }
    }
  },
  {
    $lookup: {
      from: "employees",
      localField: "_id",
      foreignField: "department_id",
      as: "employees"
    }
  },
  { $unwind: "$employees" },
  {
    $match: {
      $expr: { $gt: ["$employees.salary", "$avgSalary"] }
    }
  },
  {
    $lookup: {
      from: "departments",
      localField: "_id",
      foreignField: "_id",
      as: "department"
    }
  },
  { $unwind: "$department" },
  {
    $project: {
      _id: 0,
      name: "$employees.name",
      salary: "$employees.salary",
      department: "$department.name",
      avgSalary: 1
    }
  }
]).forEach(printjson)
