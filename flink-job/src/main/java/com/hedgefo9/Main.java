package com.hedgefo9;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String kafkaServers = params.get("kafka.bootstrap.servers", "kafka:9092");
        String kafkaTopic   = params.get("kafka.topic",       "mock_data_topic");
        String pgUrl        = params.get("postgres.url",       "jdbc:postgresql://postgres:5432/postgres");
        String pgUser       = params.get("postgres.user",      "postgres");
        String pgPassword   = params.get("postgres.password",  "postgres");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaServers);
        kafkaProps.setProperty("group.id", "flink-star-schema-group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                kafkaTopic,
                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromEarliest();

        DataStream<String> stream = env.addSource(kafkaConsumer);
        stream.flatMap(new StarSchemaWriter(pgUrl, pgUser, pgPassword)).setParallelism(1);
        env.execute("Flink Star Schema Streaming Job");
    }

    public static class StarSchemaWriter extends RichFlatMapFunction<String, Void> {
        private transient Connection conn;
        private transient ObjectMapper mapper;
        private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_LOCAL_DATE;
        private static final DateTimeFormatter MDY = DateTimeFormatter.ofPattern("M/d/yyyy");

        private final String url, user, pass;
        public StarSchemaWriter(String u, String us, String p) {
            url = u; user = us; pass = p;
        }

        @Override
        public void open(Configuration cfg) throws Exception {
            mapper = new ObjectMapper();
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, user, pass);
            conn.setAutoCommit(false);
        }

        @Override
        public void flatMap(String json, Collector<Void> out) throws Exception {
            JsonNode n = mapper.readTree(json);

            // Разбор даты продажи
            LocalDate saleDate = parseDate(n.get("sale_date").asText());
            // Ключи
            long custKey    = n.get("sale_customer_id").asLong();
            long sellerKey  = n.get("sale_seller_id").asLong();
            long productKey = n.get("sale_product_id").asLong();
            int storeKey    = n.get("store_name").asText().hashCode();
            int supplierKey = n.get("supplier_name").asText().hashCode();
            int dateKey     = Math.toIntExact(saleDate.toEpochDay());

            // 1) dim_customer
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO star_schema.dim_customer " +
                            "(customer_key, customer_id, first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                            "ON CONFLICT (customer_key) DO NOTHING"
            )) {
                ps.setLong(1, custKey);
                ps.setLong(2, custKey);
                ps.setString(3, n.get("customer_first_name").asText());
                ps.setString(4, n.get("customer_last_name").asText());
                if (n.get("customer_age").isNull()) ps.setNull(5, Types.INTEGER);
                else ps.setInt(5, n.get("customer_age").asInt());
                ps.setString(6, n.get("customer_email").asText());
                ps.setString(7, n.get("customer_country").asText());
                ps.setString(8, n.get("customer_postal_code").asText());
                ps.setString(9, n.get("customer_pet_type").asText());
                ps.setString(10, n.get("customer_pet_name").asText());
                ps.setString(11, n.get("customer_pet_breed").asText());
                ps.executeUpdate();
            }

            // 2) dim_seller
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO star_schema.dim_seller " +
                            "(seller_key, seller_id, first_name, last_name, email, country, postal_code) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                            "ON CONFLICT (seller_key) DO NOTHING"
            )) {
                ps.setLong(1, sellerKey);
                ps.setLong(2, sellerKey);
                ps.setString(3, n.get("seller_first_name").asText());
                ps.setString(4, n.get("seller_last_name").asText());
                ps.setString(5, n.get("seller_email").asText());
                ps.setString(6, n.get("seller_country").asText());
                ps.setString(7, n.get("seller_postal_code").asText());
                ps.executeUpdate();
            }

            // 3) dim_product (с учётом новых полей pet_category и product_description)
            LocalDate rel = n.get("product_release_date").asText().isEmpty() ? null : parseDate(n.get("product_release_date").asText());
            LocalDate exp = n.get("product_expiry_date").asText().isEmpty()  ? null : parseDate(n.get("product_expiry_date").asText());
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO star_schema.dim_product " +
                            "(product_key, product_id, name, category, price, weight, color, size, brand, material, rating, reviews, pet_category, product_description, product_release_date, product_expiry_date, supplier_name) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                            "ON CONFLICT (product_key) DO NOTHING"
            )) {
                ps.setLong(1, productKey);
                ps.setLong(2, productKey);
                ps.setString(3, n.get("product_name").asText());
                ps.setString(4, n.get("product_category").asText());
                if (n.get("product_price").isNull()) ps.setNull(5, Types.NUMERIC);
                else ps.setBigDecimal(5, new java.math.BigDecimal(n.get("product_price").asDouble()));
                if (n.get("product_weight").isNull()) ps.setNull(6, Types.NUMERIC);
                else ps.setBigDecimal(6, new java.math.BigDecimal(n.get("product_weight").asDouble()));
                ps.setString(7, n.get("product_color").asText());
                ps.setString(8, n.get("product_size").asText());
                ps.setString(9, n.get("product_brand").asText());
                ps.setString(10, n.get("product_material").asText());
                if (n.get("product_rating").isNull()) ps.setNull(11, Types.NUMERIC);
                else ps.setBigDecimal(11, new java.math.BigDecimal(n.get("product_rating").asDouble()));
                if (n.get("product_reviews").isNull()) ps.setNull(12, Types.INTEGER);
                else ps.setInt(12, n.get("product_reviews").asInt());
                ps.setString(13, n.get("pet_category").asText());
                ps.setString(14, n.get("product_description").asText());
                if (rel != null) ps.setDate(15, Date.valueOf(rel));
                else ps.setNull(15, Types.DATE);
                if (exp != null) ps.setDate(16, Date.valueOf(exp));
                else ps.setNull(16, Types.DATE);
                ps.setString(17, n.get("supplier_name").asText());
                ps.executeUpdate();
            }

            // 4) dim_store
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO star_schema.dim_store " +
                            "(store_key, store_name, location, city, state, country, phone, email) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                            "ON CONFLICT (store_key) DO NOTHING"
            )) {
                ps.setInt(1, storeKey);
                ps.setString(2, n.get("store_name").asText());
                ps.setString(3, n.get("store_location").asText());
                ps.setString(4, n.get("store_city").asText());
                ps.setString(5, n.get("store_state").asText());
                ps.setString(6, n.get("store_country").asText());
                ps.setString(7, n.get("store_phone").asText());
                ps.setString(8, n.get("store_email").asText());
                ps.executeUpdate();
            }

            // 5) dim_supplier
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO star_schema.dim_supplier " +
                            "(supplier_key, supplier_name, contact, email, phone, address, city, country) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                            "ON CONFLICT (supplier_key) DO NOTHING"
            )) {
                ps.setInt(1, supplierKey);
                ps.setString(2, n.get("supplier_name").asText());
                ps.setString(3, n.get("supplier_contact").asText());
                ps.setString(4, n.get("supplier_email").asText());
                ps.setString(5, n.get("supplier_phone").asText());
                ps.setString(6, n.get("supplier_address").asText());
                ps.setString(7, n.get("supplier_city").asText());
                ps.setString(8, n.get("supplier_country").asText());
                ps.executeUpdate();
            }

            // 6) dim_date
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO star_schema.dim_date " +
                            "(date_key, sale_date, year, month, day) " +
                            "VALUES (?, ?, ?, ?, ?) " +
                            "ON CONFLICT (date_key) DO NOTHING"
            )) {
                ps.setInt(1, dateKey);
                ps.setDate(2, Date.valueOf(saleDate));
                ps.setInt(3, saleDate.getYear());
                ps.setInt(4, saleDate.getMonthValue());
                ps.setInt(5, saleDate.getDayOfMonth());
                ps.executeUpdate();
            }

            // 7) fact_sales
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO star_schema.fact_sales " +
                            "(sale_id, customer_key, seller_key, product_key, store_key, supplier_key, date_key, sale_quantity, sale_total_price) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                            "ON CONFLICT (sale_id) DO NOTHING"
            )) {
                ps.setLong(1, n.get("id").asLong());
                ps.setLong(2, custKey);
                ps.setLong(3, sellerKey);
                ps.setLong(4, productKey);
                ps.setInt(5, storeKey);
                ps.setInt(6, supplierKey);
                ps.setInt(7, dateKey);
                if (n.get("sale_quantity").isNull()) ps.setNull(8, Types.INTEGER);
                else ps.setInt(8, n.get("sale_quantity").asInt());
                if (n.get("sale_total_price").isNull()) ps.setNull(9, Types.NUMERIC);
                else ps.setBigDecimal(9, new java.math.BigDecimal(n.get("sale_total_price").asDouble()));
                ps.executeUpdate();
            }

            conn.commit();
        }

        @Override
        public void close() throws Exception {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        private LocalDate parseDate(String s) {
            try {
                if (s.contains("/")) return LocalDate.parse(s, MDY);
                return LocalDate.parse(s, ISO);
            } catch (Exception e) {
                return LocalDate.parse(s, ISO);
            }
        }
    }
}
