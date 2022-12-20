package structuredapi

import org.apache.spark.sql.SparkSession

object W11_SQlDataFrame extends App {
  val spark = SparkSession.builder().appName("SQl Df write modes").master("yarn").enableHiveSupport().getOrCreate()
  val employeesDf = spark.read.format("parquet").option("path", "/user/itv000285/datasets/employee_parquet/employees").load()
  val departmentsDf = spark.read.format("parquet").option("path", "/user/itv000285/datasets/employee_parquet/departments").load()
  val dept_empDf = spark.read.format("parquet").option("path", "/user/itv000285/datasets/employee_parquet/dept_emp").load()
  val dept_managerDf = spark.read.format("parquet").option("path", "/user/itv000285/datasets/employee_parquet/dept_manager").load()
  val salariesDf = spark.read.format("parquet").option("path", "/user/itv000285/datasets/employee_parquet/salaries").load()
  val titlesDf = spark.read.format("parquet").option("path", "/user/itv000285/datasets/employee_parquet/titles").load()

  employeesDf.createOrReplaceTempView("employees")
  departmentsDf.createOrReplaceTempView("departments")
  dept_empDf.createOrReplaceTempView("dept_emp")
  dept_managerDf.createOrReplaceTempView("dept_manager")
  salariesDf.createOrReplaceTempView("salaries")
  titlesDf.createOrReplaceTempView("titles")

  val denormalizedDf = spark.sql(
    """
      |select
      |	e.first_name as first_name,
      |	e.last_name as last_name,
      |	e.birth_date as birth_date,
      |	e.gender as gender,
      |	e.hire_date as hire-date,
      |	d1.dept_name as emp_dept_name,
      |	de.from_date as emp_tenture_eff_date,
      |	de.to_date as emp_tenture_exp_date,
      |	d2.dept_name as mgr_dept_name,
      |	dm.from_date as mgr_tenture_eff_date,
      |	dm.to_date as mgr_tenture_exp_date,
      |	s.salary as emp_salary,
      |	s.from_date as salary_eff_date,
      |    s.to_date as salary_exp_date,
      |    t.title as title,
      |    t.from_date as title_eff_date,
      |    t.to_date as title_exp_date,
      |    CURRENT_TIMESTAMP() as audit_ts,
      |    CURRENT_DATE() as load_date
      |from
      |	employees e
      |left join
      |	dept_emp de
      |on
      |	e.emp_no = de.emp_no
      |left join
      |	departments d1
      |on
      |	de.dept_no = d1.dept_no
      |left join
      |	dept_manager dm
      |on
      |	e.emp_no = dm.emp_no
      |left join
      |	departments d2
      |on
      |	dm.dept_no = d2.dept_no
      |left join
      |	salaries s
      |on
      |	e.emp_no = s.emp_no
      |left join
      |	titles t
      |on
      |	e.emp_no = t.emp_no
      |""".stripMargin)

    // create a single file in hdfs path using write api
    denormalizedDf.coalesce(1).write.format("csv")
      .option("header",true).option("mode","Overwrite")
      .option("path","/user/itv000285/datasets/output").save()

  // create a file partitioned by columns in hdfs path using write api
  denormalizedDf.coalesce(4).write.partitionBy("load_date","gender")
      .format("parquet").mode("overwrite")
      .option("path","/user/itv000285/output/emp_partitioned_c").save()

  // create a file partitioned by columns in hdfs path using write api
  denormalizedDf.write.partitionBy("load_date").bucketBy(4,"first_name")
    .format("parquet").mode("overwrite")
    .option("path", "/user/itv000285/output/emp_partitioned_c").saveAsTable("jeevichi.employee_master_c")
}
