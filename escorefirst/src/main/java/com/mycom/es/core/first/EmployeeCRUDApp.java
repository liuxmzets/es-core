package com.mycom.es.core.first;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * 员工增删改查的应用程序
 */
public class EmployeeCRUDApp {
    public static void main(String[] args) throws Exception{
        //先构建client
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(
                        InetAddress.getByName("localhost")
                        , 9300
                ));


        createEmployee(client);
//        getEmployee(client);

//        updateEmployee(client);
//        deleteEmployee(client);


        client.close();
    }

    private static void createEmployee(TransportClient client) throws Exception{
        IndexResponse response = client.prepareIndex("company", "employee", "2")
                .setSource(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field("name", "jack2")
                                .field("age", 272)
                                .field("position", "technique2")
                                .field("country", "china2")
                                .field("join_date", "2017-01-02")
                                .field("salary", 10002)
                                .endObject()
                ).get();
        System.out.println(response.getResult());
    }

    //获取员工信息

    private static void getEmployee(TransportClient client)
        throws Exception{
        GetResponse response = client.prepareGet("company", "employee", "1")
                .get();
        System.out.println(response.getSourceAsString());
    }

    //修改员工信息
    private static void updateEmployee(TransportClient client) throws Exception{
        UpdateResponse response = client.prepareUpdate("company", "employee", "1")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("position", "technique manager")
                        .endObject())
                .get();

        System.out.println(response.getResult());
    }

    //删除员工信息
    private static void deleteEmployee(TransportClient client) {
        DeleteResponse response = client.prepareDelete("company","employee", "1").get();
        System.out.println(response.getResult());
    }
}
