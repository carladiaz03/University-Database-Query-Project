/*****************************
Query the University Database
*****************************/
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.util.*;
import java.lang.String;

public class MyQuery {

    private Connection conn = null;
	 private Statement statement = null;
	 private ResultSet resultSet = null;
    
    public MyQuery(Connection c)throws SQLException
    {
        conn = c;
        // Statements allow to issue SQL queries to the database
        statement = conn.createStatement();
    }
    
    public void findFall2009Students() throws SQLException
    {
        String query  = "select distinct name from student natural join takes where semester = \'Fall\' and year = 2009;";

        resultSet = statement.executeQuery(query);
    }
    
    public void printFall2009Students() throws IOException, SQLException
    {
	      System.out.println("******** Query 0 ********");
         System.out.println("name");
         while (resultSet.next()) {
			// It is possible to get the columns via name
			// also possible to get the columns via the column number which starts at 1
			String name = resultSet.getString(1);
         System.out.println(name);
   		}        
    }

    public void findGPAInfo() throws SQLException {
        String query = "SELECT student.id, student.name, " +
                "ROUND(SUM(course.credits * CASE takes.grade " +
                "WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7 WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7 " +
                "WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 WHEN 'C-' THEN 1.7 WHEN 'D+' THEN 1.3 WHEN 'D' THEN 1.0 " +
                "WHEN 'D-' THEN 0.7 WHEN 'F' THEN 0 ELSE NULL END) / SUM(course.credits), 2) AS GPA " +
                "FROM student " +
                "JOIN takes ON student.id = takes.id " +
                "JOIN course ON takes.course_id = course.course_id " +
                "WHERE takes.grade IS NOT NULL " +
                "GROUP BY student.id, student.name;";
        resultSet = statement.executeQuery(query);
    }


    public void printGPAInfo() throws IOException, SQLException {
        System.out.println("******** Query 1 ********");
        System.out.println("id | name | GPA");
        while (resultSet.next()) {
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            double gpa = resultSet.getDouble("GPA");
            System.out.printf("%s | %s | %.2f%n", id, name, gpa);
        }
    }

    public void findMorningCourses() throws SQLException {
        String query =
                "SELECT c.course_id, s.sec_id, c.title, s.semester, s.year, " +
                        "i.name, COUNT(DISTINCT tk.id) AS enrollment " +
                        "FROM course c " +
                        "JOIN section s ON c.course_id = s.course_id " +
                        "JOIN time_slot ts ON s.time_slot_id = ts.time_slot_id " +
                        "JOIN teaches t ON s.course_id = t.course_id AND s.sec_id = t.sec_id AND s.semester = t.semester AND s.year = t.year " +
                        "JOIN instructor i ON t.id = i.id " +
                        "JOIN takes tk ON s.course_id = tk.course_id AND s.sec_id = tk.sec_id AND s.semester = tk.semester AND s.year = tk.year " +
                        "WHERE ts.start_hr < 12 " +
                        "GROUP BY c.course_id, s.sec_id, c.title, s.semester, s.year, i.name " +
                        "HAVING COUNT(DISTINCT tk.id) > 0 AND COUNT(DISTINCT tk.id) < 6;";
        resultSet = statement.executeQuery(query);
    }




    public void printMorningCourses() throws IOException, SQLException {
        System.out.println("******** Query 2 ********");
        System.out.println("course_id | sec_id | title | semester | year | name | enrollment");
        while (resultSet.next()) {
            String courseId = resultSet.getString("course_id");
            int sectionId = resultSet.getInt("sec_id");
            String title = resultSet.getString("title");
            String semester = resultSet.getString("semester");
            int year = resultSet.getInt("year");
            String name = resultSet.getString("name");
            int enrollment = resultSet.getInt("enrollment");

            System.out.printf("%s | %d | %s | %s | %d | %s | %d%n",
                    courseId, sectionId, title, semester, year, name, enrollment);
        }
    }



    public void findBusyClassroom() throws SQLException {
        String query =
                "SELECT s.building, s.room_number, COUNT(*) AS frequency " +
                        "FROM section s " +
                        "GROUP BY s.building, s.room_number " +
                        "HAVING COUNT(*) = (" +
                        "  SELECT MAX(frequency) " +
                        "  FROM (" +
                        "    SELECT COUNT(*) AS frequency " +
                        "    FROM section " +
                        "    GROUP BY building, room_number" +
                        "  ) AS subquery" +
                        ");";
        resultSet = statement.executeQuery(query);
    }


    public void printBusyClassroom() throws IOException, SQLException {
        System.out.println("******** Query 3 ********");
        System.out.println("building | room_number | frequency");
        while (resultSet.next()) {
            String building = resultSet.getString("building");
            String roomNumber = resultSet.getString("room_number");
            int frequency = resultSet.getInt("frequency");
            System.out.printf("%-10s | %-11s | %-9d%n", building, roomNumber, frequency);
        }
    }



    public void findPrereq() throws SQLException {
        String query =
                "SELECT c1.title AS course, COALESCE(c2.title, 'N/A') AS prereq " +
                        "FROM course c1 " +
                        "LEFT JOIN prereq p ON c1.course_id = p.course_id " +
                        "LEFT JOIN course c2 ON p.prereq_id = c2.course_id;";
        resultSet = statement.executeQuery(query);
    }

    public void printPrereq() throws IOException, SQLException {
        System.out.println("******** Query 4 ********");
        System.out.println("course | prereq");
        while (resultSet.next()) {
            String course = resultSet.getString("course");
            String prereq = resultSet.getString("prereq");
            System.out.printf("%s | %s%n", course, prereq);
        }
    }




    public void updateTable() throws SQLException {
        // Create a copy of the student table named studentCopy
        String createTable = "CREATE TABLE IF NOT EXISTS studentCopy AS SELECT * FROM student;";

        // Update the tot_cred field in the studentCopy table
        String updateCredits = "UPDATE studentCopy AS sc " +
                "JOIN (SELECT id, IFNULL(SUM(credits), 0) AS total_credits " +
                "      FROM takes " +
                "      JOIN course ON takes.course_id = course.course_id " +
                "      WHERE grade IS NOT NULL AND grade != 'F' " +
                "      GROUP BY id) AS temp " +
                "ON sc.id = temp.id " +
                "SET sc.tot_cred = temp.total_credits;";

        // Execute the SQL queries
        statement.executeUpdate(createTable);
        statement.executeUpdate(updateCredits);
    }

    public void printUpdatedTable() throws IOException, SQLException {
        // Select data from the studentCopy table and count the number of distinct courses each student has taken
        String query = "SELECT sc.id, sc.name, sc.tot_cred, COUNT(DISTINCT t.course_id) AS num_courses, d.dept_name " +
                "FROM studentCopy sc " +
                "LEFT JOIN takes t ON sc.id = t.id " +
                "JOIN department d ON sc.dept_name = d.dept_name " +
                "GROUP BY sc.id, sc.name, sc.tot_cred, d.dept_name;";

        // Execute the query and retrieve the result set
        resultSet = statement.executeQuery(query);

        // Print the column headers
        System.out.println("******** Query 5 ********");
        System.out.println("id | name | dept_name | tot_cred | num_courses");

        // Iterate through the result set and print each row
        while (resultSet.next()) {
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            String deptName = resultSet.getString("dept_name");
            int totCred = resultSet.getInt("tot_cred");
            int numCourses = resultSet.getInt("num_courses");
            System.out.printf("%s | %s | %s | %d | %d%n", id, name, deptName, totCred, numCourses);
        }
    }



    public void findDeptInfo() throws SQLException {
        System.out.println("******** Query 6 ********");
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter department name:");
        String deptName = scanner.nextLine();

        CallableStatement cstmt = null;
        try {
            // Prepare the callable statement to call the stored procedure
            cstmt = conn.prepareCall("{CALL deptInfo(?, ?, ?, ?)}");
            cstmt.setString(1, deptName);
            cstmt.registerOutParameter(2, java.sql.Types.INTEGER);
            cstmt.registerOutParameter(3, java.sql.Types.DECIMAL);
            cstmt.registerOutParameter(4, java.sql.Types.DECIMAL);

            // Execute the stored procedure
            cstmt.execute();

            // Retrieve the output parameters
            int numInstructors = cstmt.getInt(2);
            double totalSalary = cstmt.getDouble(3);
            double budget = cstmt.getDouble(4);

            // Print the results
            System.out.printf("Department: %s%n", deptName);
            System.out.printf("Number of Instructors: %d%n", numInstructors);
            System.out.printf("Total Salary: %.2f%n", totalSalary);
            System.out.printf("Budget: %.2f%n", budget);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (cstmt != null) {
                cstmt.close();
            }
        }
    }


    public void findFirstLastSemester() throws SQLException
    {
        String query = "SELECT id, name, " +
                "MIN(CONCAT(year, ' ', semester)) AS first_semester, " +
                "MAX(CONCAT(year, ' ', semester)) AS last_semester " +
                "FROM student NATURAL JOIN takes " +
                "GROUP BY id, name;";
        resultSet = statement.executeQuery(query);
    }

    public void printFirstLastSemester() throws IOException, SQLException
    {
        System.out.println("******** Query 7 ********");
        System.out.println("id | name | first_semester | last_semester");
        while (resultSet.next()) {
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            String firstSemester = resultSet.getString("first_semester");
            String lastSemester = resultSet.getString("last_semester");
            System.out.printf("%s | %s | %s | %s%n", id, name, firstSemester, lastSemester);
        }
    }

}
