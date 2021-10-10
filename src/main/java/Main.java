import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    static Connection dbConnection;
    static String dbHost = "localhost";
    static String dbPort = "5432";
    static String dbUser = "postgres";
    static String dbPass = "Password";
    static String dbName = "database1";

    public static Connection getDbConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        String connectionString = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName;
        dbConnection = DriverManager.getConnection(connectionString, dbUser, dbPass);
        return dbConnection;
    }

    public static void printResultSet(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + " " + resultSet.getString(2)
                    + " " + resultSet.getString(3));
        }
    }

    public static void printDelimiter() {
        System.out.println("-------------------------------------");
    }

    public static void findPeopleRegisteredInGivenFlat(int flat_id) throws SQLException {
        System.out.println("Люди, прописанные в заданной квартире:");
        String query = "SELECT last_name, first_name, second_name FROM persons " +
                "JOIN flat_registered ON persons.person_id = flat_registered.person_id " +
                "WHERE flat_id = ?";
        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, flat_id);
        ResultSet resultSet = statement.executeQuery();
        printResultSet(resultSet);
    }

    public static void findPeopleOwningGivenFlat(int flat_id) throws SQLException {
        System.out.println("Люди, владеющие заданной квартирой:");
        String query = "SELECT last_name, first_name, second_name FROM persons " +
                "JOIN flat_owners ON persons.person_id = flat_owners.person_id " +
                "WHERE flat_id = ?;";
        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, flat_id);
        ResultSet resultSet = statement.executeQuery();
        printResultSet(resultSet);
    }

    public static void findPeopleRegisteredInGivenCity(int city_id) throws SQLException {
        System.out.println("Люди, прописанные в заданном городе:");
        String query = "SELECT last_name, first_name, second_name FROM persons " +
                "JOIN flat_registered ON persons.person_id = flat_registered.person_id " +
                "JOIN flats ON flat_registered.flat_id = flats.flat_id " +
                "JOIN houses ON house = house_id " +
                "JOIN streets ON street = street_id " +
                "JOIN cities ON city = city_id " +
                "WHERE city_id = ?";
        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, city_id);
        ResultSet resultSet = statement.executeQuery();
        printResultSet(resultSet);
    }

    public static void findPeopleRegisteredInGivenHouse(int house_id) throws SQLException {
        System.out.println("Люди, прописанные в заданном доме:");
        String query = "SELECT last_name, first_name, second_name FROM persons " +
                "JOIN flat_registered ON persons.person_id = flat_registered.person_id " +
                "JOIN flats ON flat_registered.flat_id = flats.flat_id " +
                "JOIN houses ON house = house_id " +
                "WHERE house_id = ?";
        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, house_id);
        ResultSet resultSet = statement.executeQuery();
        printResultSet(resultSet);
    }

    public static void findPeopleRegisteredOnGivenListOfStreets(List<Integer> streetList) throws SQLException {
        System.out.println("Люди, прописанные на улицах из заданного списка:");
        String list = "(" + streetList.stream().map(String::valueOf).collect(Collectors.joining(", ")) + ");";
        String query = "SELECT last_name, first_name, second_name FROM persons " +
                "JOIN flat_registered ON persons.person_id = flat_registered.person_id " +
                "JOIN flats ON flat_registered.flat_id = flats.flat_id " +
                "JOIN houses ON house = house_id " +
                "JOIN streets ON street = street_id " +
                "WHERE street_id in " + list;
        Statement statement = dbConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        printResultSet(resultSet);
    }

    public static void findHomelessFromGivenCity(int city_id) throws SQLException {
        System.out.println("Люди без определенного места жительства из заданного города: ");
        String query = "SELECT last_name, first_name, second_name FROM persons " +
                "JOIN city_of_residence ON persons.person_id = city_of_residence.person_id " +
                "LEFT JOIN flat_registered ON persons.person_id = flat_registered.person_id " +
                "LEFT JOIN flat_owners ON persons.person_id = flat_owners.person_id " +
                "WHERE city_of_residence.city_id = ? " +
                "AND flat_registered.person_id IS NULL " +
                "AND flat_owners.person_id IS NULL;";
        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, city_id);
        ResultSet resultSet = statement.executeQuery();
        printResultSet(resultSet);
    }

    public static void deletePersonFromFlat(int person_id, int flat_id) throws SQLException {
        String query = "DELETE FROM flat_registered " +
                "WHERE person_id = ? AND flat_id = ?;";
        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, person_id);
        statement.setInt(2, flat_id);
        statement.executeUpdate();
        System.out.println("Человек выписан");
    }

    public static void registerPersonInTheFlat(int person_id, int flat_id) throws SQLException {
        String query = "INSERT INTO flat_registered (person_id, flat_id) " +
                "VALUES (?, ?);";
        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, person_id);
        statement.setInt(2, flat_id);
        statement.executeUpdate();
        System.out.println("Человек прописан");
    }

    public static void moveToNewFlat(int newFlat_id, int oldFlat_id) throws SQLException {
        String query = "UPDATE flat_registered " +
                "SET flat_id = ? " +
                "WHERE flat_id = ?;";
        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, newFlat_id);
        statement.setInt(2, oldFlat_id);
        statement.executeUpdate();
        System.out.println("ID квартиры обновлен");
    }

    public static void changeFlats(int flat1_id, int flat2_id) throws SQLException {
        String query = "WITH flat_numbers (flat1, flat2) AS ( " +
                "   VALUES (?, ?) " + ") " +
                "UPDATE flat_registered " +
                "SET flat_id =  " +
                    "CASE  " +
                        "WHEN flat_id = (SELECT flat1 FROM flat_numbers) THEN (SELECT flat2 FROM flat_numbers) " +
                        "WHEN flat_id = (SELECT flat2 FROM flat_numbers) THEN (SELECT flat1 FROM flat_numbers) " +
                    "END " +
                "WHERE flat_id IN ((SELECT flat1 FROM flat_numbers), (SELECT flat2 FROM flat_numbers));";
        PreparedStatement statement = dbConnection.prepareStatement(query);
        statement.setInt(1, flat1_id);
        statement.setInt(2, flat2_id);
        statement.executeUpdate();
        System.out.println("Обмен осуществлен");
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Connection connection = getDbConnection();
        findPeopleRegisteredInGivenFlat(1);
        printDelimiter();
        findPeopleOwningGivenFlat(1);
        printDelimiter();
        findPeopleRegisteredInGivenCity(1);
        printDelimiter();
        findPeopleRegisteredInGivenHouse(1);
        printDelimiter();
        findPeopleRegisteredOnGivenListOfStreets(List.of(1, 4, 7));
        printDelimiter();
        findHomelessFromGivenCity(2);
        printDelimiter();
        deletePersonFromFlat(13, 7);
        printDelimiter();
        registerPersonInTheFlat(13, 7);
        printDelimiter();
        moveToNewFlat(4, 1);
        printDelimiter();
        changeFlats(1, 3);
        connection.close();
    }
}
