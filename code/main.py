import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col, sum, rank, desc, split, trim
from pyspark.sql.window import Window


class crashDataAnalysis:
    def __init__(self, path_to_config_file):
        """
        Read the data from the csv files and create data frames
        :param path_to_config_file: Any
        :return: None
        """

        with open(path_to_config_file, encoding='utf-8') as f:
            config_file = yaml.safe_load(f)
        self.df_Charges = spark.read.format("csv").option("header", "true").load(
            config_file["paths"]["input"]["Charges"])
        self.df_Damages = spark.read.format("csv").option("header", "true").load(
            config_file["paths"]["input"]["Damages"])
        self.df_Endorse = spark.read.format("csv").option("header", "true").load(
            config_file["paths"]["input"]["Endorse"])
        self.df_Primary_Person = spark.read.format("csv").option("header", "true").load \
            (config_file["paths"]["input"]["Primary_Person"])
        self.df_Restrict = spark.read.format("csv").option("header", "true").load(
            config_file["paths"]["input"]["Restrict"])
        self.df_Units = spark.read.format("csv").option("header", "true").load(config_file["paths"]["input"]["Units"])

    def get_total_male_deaths(self):
        """
        Find the total number of crashes in which persons killed are male
        :return: int
        """

        killed_male = self.df_Primary_Person.filter(
            (upper(self.df_Primary_Person.PRSN_INJRY_SEV_ID) == "KILLED") & (
                    upper(self.df_Primary_Person.PRSN_GNDR_ID) == "MALE"))
        killed_male = killed_male.select(sum(col("DEATH_CNT"))).collect()[0][0]
        return int(killed_male)

    def get_two_wheeler_crashes(self):
        """
        Find the number of two-wheelers are booked for crashes
        :return: int
        """

        two_wheelers = self.df_Units.filter(upper(col("VEH_BODY_STYL_ID")).contains("MOTORCYCLE"))
        two_wheelers_count = str(int(two_wheelers.count()))
        return two_wheelers_count

    def get_state_with_highest_female_accidents(self):
        """
        Find state where highest number of female accidents are reported
        :return: int
        """

        female_accidents = self.df_Primary_Person.filter(upper(self.df_Primary_Person.PRSN_GNDR_ID) == "FEMALE") \
            .groupby(self.df_Primary_Person.DRVR_LIC_STATE_ID).count() \
            .orderBy(col("count").desc())
        high_female_accident_state = female_accidents.collect()[0][0]
        return high_female_accident_state

    def top_5_to_15_veh_with_injuries_and_deaths(self):
        """
        Find Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        :return: string
        """

        top_vehicles = self.df_Units.filter(self.df_Units.VEH_MAKE_ID != "NA") \
            .withColumn("TOTAL_INJURY_COUNT_INCLUDING_DEATH", self.df_Units.TOT_INJRY_CNT + self.df_Units.DEATH_CNT) \
            .groupBy("VEH_MAKE_ID").sum("TOTAL_INJURY_COUNT_INCLUDING_DEATH") \
            .withColumnRenamed("sum(TOTAL_INJURY_COUNT_INCLUDING_DEATH)", "TOTAL_INJURY_COUNT_PER_VEH_MAKE_ID") \
            .withColumn("RANK", rank().over(Window.orderBy(desc("TOTAL_INJURY_COUNT_PER_VEH_MAKE_ID")))) \
            .filter((col("RANK") >= 5) & (col("RANK") <= 15))
        i = 1
        veh_make_ids = ""
        for veh in top_vehicles.select("VEH_MAKE_ID").collect():
            if i != top_vehicles.count():
                veh_make_ids = veh_make_ids + str(veh[0]) + ", "
            else:
                veh_make_ids = veh_make_ids + str(veh[0])
            i = i + 1
        return veh_make_ids

    def top_ethnic_user_group_of_each_unique_body_style(self):
        """
        Find top ethnic user group of each unique body style
        :return: DataFrame
        """

        top_ethnic_user_group = self.df_Units.join(self.df_Primary_Person, on=["CRASH_ID"], how="inner") \
            .filter(
            ~self.df_Units.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"])) \
            .filter(~self.df_Primary_Person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])) \
            .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count() \
            .withColumn("RANK", rank().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc()))) \
            .filter(col("RANK") == 1).drop("count", "RANK")

        return top_ethnic_user_group

    def get_top_5_zip_codes_highest_number_crashes_with_alcohols_contributing_factor(self):
        """
        Find Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash
        :return: string
        """

        top_5_zip_codes = self.df_Units.join(self.df_Primary_Person, on=["CRASH_ID"], how="inner") \
            .dropna(subset=["DRVR_ZIP"]) \
            .filter(col("VEH_BODY_STYL_ID").contains("CAR")) \
            .filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")) \
            .groupBy("DRVR_ZIP").count() \
            .withColumn("RANK", rank().over(Window.orderBy(col("count").desc()))) \
            .filter(col("RANK") <= 5).drop("count", "RANK")

        i = 1
        zip_codes = ""
        for zip in top_5_zip_codes.select("DRVR_ZIP").collect():
            if i != top_5_zip_codes.count():
                zip_codes = zip_codes + str(zip[0]) + ", "
            else:
                zip_codes = zip_codes + str(zip[0])
            i = i + 1
        return zip_codes

    def get_count_of_crashes_with_damage_level_above_4_and_no_property_damage(self):
        """
        Find Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is
        above 4 and car avails Insurance
        :return: int
        """

        crash_ids = self.df_Damages.join(self.df_Units, on=["CRASH_ID"], how="inner") \
            .filter((split(col("VEH_DMAG_SCL_1_ID"), " ")[1].cast("int") >= 4) | (
                split(col("VEH_DMAG_SCL_2_ID"), " ")[1].cast("int") >= 4)) \
            .filter((upper(col("DAMAGED_PROPERTY")) == "NONE") | (trim(col("DAMAGED_PROPERTY")) == "") | (
                upper(col("DAMAGED_PROPERTY")) == "NONE1")) \
            .filter(upper(col("FIN_RESP_TYPE_ID")) == "PROOF OF LIABILITY INSURANCE").select("CRASH_ID").distinct()

        return crash_ids.count()

    def get_top_5_veh_make_id(self):
        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers,
        used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences
        :return: int
        """
        top_25_states = [STATE_ID[0] for STATE_ID in self.df_Units.filter(col("VEH_LIC_STATE_ID") != "NA")
        .groupby("VEH_LIC_STATE_ID").count()
        .withColumn("RANK", rank().over(Window.orderBy(col("count").desc())))
        .filter(col("RANK") <= 25).collect()]

        top_10_veh_colors = [COLOUR_ID[0] for COLOUR_ID in self.df_Units.filter(col("VEH_COLOR_ID") != "NA")
        .groupby("VEH_COLOR_ID").count()
        .withColumn("RANK", rank().over(Window.orderBy(col("count").desc())))
        .filter(col("RANK") <= 10).collect()]

        top_5_veh_make_id = self.df_Charges.join(self.df_Primary_Person, on=["CRASH_ID"], how='inner') \
            .join(self.df_Units, on=["CRASH_ID"], how='inner') \
            .filter(col("CHARGE").contains("SPEED")) \
            .filter(~(col("DRVR_LIC_TYPE_ID").isin("UNLICENSED", "UNKNOWN", "NA"))) \
            .filter((col("VEH_COLOR_ID").isin(top_10_veh_colors)) & (col("VEH_LIC_STATE_ID").isin(top_25_states))) \
            .groupBy("VEH_MAKE_ID").count() \
            .withColumn("RANK", rank().over(Window.orderBy(col("count").desc()))) \
            .filter(col("RANK") <= 5).select("VEH_MAKE_ID").distinct()

        i = 1
        make_ids = ""
        for MAKE_ID in top_5_veh_make_id.select("VEH_MAKE_ID").collect():
            if i != top_5_veh_make_id.count():
                make_ids = make_ids + str(MAKE_ID[0]) + ", "
            else:
                make_ids = make_ids + str(MAKE_ID[0])
            i = i + 1
        return make_ids


if __name__ == '__main__':
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("crashDataAnalysis") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    config_file_path = "config/config.yaml"
    Analysis = crashDataAnalysis(config_file_path)
    print("***************************************************************************")
    print("***************************************************************************")
    print("***************************************************************************")

    print("1. Number of crashes (accidents) in which number of persons killed are male")
    print(Analysis.get_total_male_deaths())
    print("")

    print("2. Number of two wheelers are booked for crashes")
    print(Analysis.get_two_wheeler_crashes())
    print("")

    print("3. State highest number of accidents in which females are involved")
    print(Analysis.get_state_with_highest_female_accidents())
    print("")

    print("4. Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death")
    print(Analysis.top_5_to_15_veh_with_injuries_and_deaths())
    print("")

    print("5. For all the body styles involved in crashes, the top ethnic user group of each unique body style")
    df = Analysis.top_ethnic_user_group_of_each_unique_body_style()
    df.show()
    print("")

    print("6. Among the crashed cars, Top 5 Zip Codes with highest number crashes with alcohols as the contributing "
          "factor to a crash")
    print(Analysis.get_top_5_zip_codes_highest_number_crashes_with_alcohols_contributing_factor())
    print("")

    print("7. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level is "
          "above 4 and car avails Insurance")
    print(Analysis.get_count_of_crashes_with_damage_level_above_4_and_no_property_damage())
    print("")

    print("8. Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, "
          "used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of "
          "offences ")
    print(Analysis.get_top_5_veh_make_id())
    print("")
    print("***************************************************************************")
    print("***************************************************************************")
    print("***************************************************************************")
