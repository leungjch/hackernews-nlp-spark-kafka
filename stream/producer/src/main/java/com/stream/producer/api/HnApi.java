package com.stream.producer.api;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.net.URI;
import java.net.http.HttpRequest;
import java.io.FileWriter;
import java.io.Writer;
import java.io.File;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class HnApi {
    /**
     * Class for sending requests to HN API
     */

    final String HN_URL = "https://hacker-news.firebaseio.com/v0";

    Gson gson = new GsonBuilder().create();

    private String sendRequest(String endpoint) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(String.format("%s%s", HN_URL, endpoint))).build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();

    }

    public String getById(int id) throws IOException, InterruptedException {
        String ret = sendRequest(String.format("/item/%s.json?print=pretty", Integer.toString(id)));
        return ret;
    }

    public int getLatestId() throws IOException, InterruptedException {
        String ret = sendRequest("/maxitem.json");

        return Integer.parseInt(ret);
    }

}
