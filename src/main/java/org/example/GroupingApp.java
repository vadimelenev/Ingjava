package org.example;

import org.h2.jdbcx.JdbcDataSource;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;
import java.util.logging.*;

public class GroupingApp {
    private static final Logger logger = Logger.getLogger(GroupingApp.class.getName());

    public static void main(String[] args) {
        if (args.length != 1) {
            logger.severe("Usage: java -jar GroupingApp.jar <input-file>");
            return;
        }

        String inputFile = args[0];
        long startTime = System.nanoTime();

        try {
            // Настройка источника данных H2
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");  // Использование памяти для базы данных
            dataSource.setUser("sa");
            dataSource.setPassword("");

            // Загружаем данные в базу данных
            loadDataToDatabase(inputFile, dataSource);

            // Параллельная обработка группировки данных
            List<Set<List<String>>> groups = groupLinesFromDatabase(dataSource);

            // Запись результатов в файл
            writeOutput(groups);
            logger.info("Total time: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");
        } catch (Exception e) {
            logger.severe("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Загружаем данные в базу данных
    private static void loadDataToDatabase(String filePath, JdbcDataSource dataSource) throws IOException, SQLException {
        try (Connection conn = dataSource.getConnection()) {
            // Создание таблицы
            String createTableSQL = "CREATE TABLE IF NOT EXISTS records (id INT AUTO_INCREMENT, data VARCHAR(255))";
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSQL);
            }

            // Чтение данных из файла и вставка в базу
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filePath))))) {
                String line;
                String insertSQL = "INSERT INTO records (data) VALUES (?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                    int lineCount = 0;
                    while ((line = reader.readLine()) != null) {
                        if (!line.trim().isEmpty()) { // Игнорируем пустые строки
                            // Убираем все кавычки (двойные и одинарные)
                            line = line.replace("\"", "").replace("'", "");
                            pstmt.setString(1, line);
                            pstmt.addBatch();
                            lineCount++;
                        }
                    }
                    pstmt.executeBatch();  // Выполнение пакетной вставки
                    logger.info("Total lines processed and loaded: " + lineCount);
                }
            }
        }
    }

    // Параллельная обработка группировки данных из базы данных с использованием Union-Find
    private static List<Set<List<String>>> groupLinesFromDatabase(JdbcDataSource dataSource) throws SQLException, InterruptedException {
        List<Set<List<String>>> groups = new ArrayList<>();

        // Получаем данные из базы данных
        try (Connection conn = dataSource.getConnection()) {
            String query = "SELECT data FROM records";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {

                List<List<String>> lines = new ArrayList<>();
                while (rs.next()) {
                    String line = rs.getString("data");
                    lines.add(Arrays.asList(line.split(";")));
                }

                // Сортировка строк по количеству элементов
                lines.sort(Comparator.comparingInt(List::size));

                // Инициализация Union-Find
                UnionFind uf = new UnionFind(lines.size());

                // Параллельная обработка для нахождения пересечений
                ExecutorService executor = Executors.newFixedThreadPool(32); // Используем 6 потоков
                List<Callable<Void>> tasks = new ArrayList<>();

                // Сравнение строк и создание графа пересечений с использованием Union-Find
                for (int i = 0; i < lines.size(); i++) {
                    int finalI = i;
                    tasks.add(() -> {
                        for (int j = finalI + 1; j < lines.size(); j++) {
                            if (hasIntersection(lines.get(finalI), lines.get(j))) {
                                uf.union(finalI, j);  // Объединяем индексы строк
                            }
                        }
                        return null;
                    });
                }

                // Выполнение всех задач параллельно
                executor.invokeAll(tasks);
                executor.shutdown();

                // Сборка групп по корням Union-Find
                Map<Integer, Set<List<String>>> groupsMap = new HashMap<>();
                for (int i = 0; i < lines.size(); i++) {
                    int root = uf.find(i);
                    groupsMap.computeIfAbsent(root, k -> new HashSet<>()).add(lines.get(i));
                }

                // Добавляем группы, где больше одного элемента
                for (Set<List<String>> group : groupsMap.values()) {
                    if (group.size() > 1) {
                        groups.add(group);
                    }
                }

                logger.info("Total groups formed: " + groups.size());
            }
        }
        return groups;
    }

    // Проверка на пересечение значений в столбцах
    private static boolean hasIntersection(List<String> line1, List<String> line2) {
        if (line1.size() != line2.size()) {
            return false;
        }
        for (int i = 0; i < line1.size(); i++) {
            String val1 = line1.get(i).trim();
            String val2 = line2.get(i).trim();

            // Проверяем, что хотя бы одно из значений непустое и совпадает
            if (!val1.isEmpty() && val1.equals(val2)) {
                return true;
            }
        }
        return false;
    }

    // Запись выходных данных в файл
    private static void writeOutput(List<Set<List<String>>> groups) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("C:\\Users\\Administrator\\Downloads\\File\\output.txt"))) {
            int groupCount = 0;

            // Сортировка групп по убыванию количества элементов
            groups.sort((group1, group2) -> Integer.compare(group2.size(), group1.size()));

            // Записываем группы в файл
            for (Set<List<String>> group : groups) {
                groupCount++;
                writer.write("Группа " + groupCount + "\n");

                // Записываем строки в группу
                for (List<String> line : group) {
                    // Проверка на пустые строки
                    if (line.stream().allMatch(String::isEmpty)) {
                        continue;
                    }
                    // Только строки с хотя бы одним непустым значением
                    writer.write(String.join(";", line) + "\n");
                }
                writer.write("\n");
            }
            logger.info("Number of groups with more than one element: " + groupCount);
        }
    }
}

