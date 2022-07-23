package com.spark.kafka.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {


    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection(){
        try {
            return DriverManager.getConnection("jdbc:mysql://hadoop2:3306/gmallflink?useSSL=false",
                    "root","root");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static PreparedStatement getPreparedStatementWithFilledParams(Connection connection,String sql,Object ...params) throws SQLException {
        PreparedStatement preparedStatement=null;
        preparedStatement=connection.prepareStatement(sql);
        if(params!=null&&params.length>0) {
            for (int i = 0; i < params.length; i++) {
                preparedStatement.setObject(i + 1, params[i] + "");
            }
        }
        return preparedStatement;
    }
    public static <T> List<T> queryList(Connection connection,Class<T> tClass,String sql,Object ...params){
        PreparedStatement preparedStatement=null;
        ResultSet resultSet=null;
        List<T> list=new ArrayList<>();
        try{
            preparedStatement=getPreparedStatementWithFilledParams(connection, sql, params);
            resultSet=preparedStatement.executeQuery();

            while (resultSet.next()){
                T t = tClass.newInstance();
                ResultSetMetaData metaData = resultSet.getMetaData();
//                for (int i = 0; i <metaData.getColumnCount() ; i++) {
//                    String columnName = metaData.getColumnName(i + 1);
//                    String value = resultSet.getString(columnName);
//
//                }
                Field[] declaredFields = tClass.getDeclaredFields();
                for (Field field : declaredFields) {
                    field.setAccessible(true);
                    field.set(t, resultSet.getObject(field.getName()));
                }
                list.add(t);

            }
        }catch (Exception e){

            e.printStackTrace();
        }finally {
            try {
            if(preparedStatement!=null) {

                    preparedStatement.close();
                }
                ;  connection.close();
            }catch (SQLException e) {
                e.printStackTrace();
            }

        }
        return list;

    }

    public static JSONArray executeSql(Connection connection,String sql,Object ...params){
        PreparedStatement preparedStatement=null;
        ResultSet resultSet=null;


        JSONArray jsonArray=new JSONArray();

        try {
             preparedStatement= getPreparedStatementWithFilledParams(connection,sql,params);

            resultSet= preparedStatement.executeQuery();

            while (resultSet.next()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
//
//                Class<?> aClass = Class.forName(className);
//                Field[] declaredFields = aClass.getDeclaredFields();
//
//                for (int i = 0; i <declaredFields.length ; i++) {
//                    Field declaredField = declaredFields[i];
//                    String value = resultSet.getString(declaredField.getName());
//
//
//                }
                JSONObject jsonObject=new JSONObject();
                for (int i = 0; i < metaData.getColumnCount() ; i++) {
                    String columnName = metaData.getColumnName(i+1);
                    jsonObject.put(columnName,resultSet.getObject(columnName));

                }

//                for (int i = 0; i < metaData.getColumnCount(); i++) {
//                    String columnLabel = metaData.getColumnLabel(i + 1);
//                    Object arg = resultSet.getString(columnLabel);
//                    Field declaredField = null;
//                    try {
//                        declaredField = aClass.getDeclaredField(columnLabel.toLowerCase());
//                    } catch (Exception e) {
//
//                    }
//                    if (declaredField != null) {
//                        declaredField.setAccessible(true);
//                        declaredField.set(obj, arg);
//
//                    }
//
////                Method declaredMethod = tClass.getDeclaredMethod("set" + columnLabel);
////
////                declaredMethod.invoke(t,arg);
//                }
                jsonArray.add(jsonObject);

            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (preparedStatement != null) preparedStatement.close();
                if(resultSet!=null)resultSet.close();;
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return jsonArray;
    }





    private static void close(Closeable ... closeables){
        for (Closeable closeable:closeables){
            if(closeable!=null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
//        List<SkuInfo> skuInfos = queryList(getPhoenixConnection(), SkuInfo.class,
//                "select *from "+PhoenixConstants.HBASE_SCHEMA+".DIM_SKU_INFO where id=?", 2);
//        System.out.println(skuInfos);
//        JSONArray jsonArray = executeSql(getPhoenixConnection(),
//                "select *from " + PhoenixConstants.HBASE_SCHEMA + ".DIM_SKU_INFO where id=?", 2);
//        JSONObject o = jsonArray.getJSONObject(0);
//        SkuInfo skuInfo = JSONObject.parseObject((String) o, SkuInfo.class);
        char[] chars = "100".toCharArray();
        System.out.println(chars);
        System.out.println(chars[0]);
    }
}
