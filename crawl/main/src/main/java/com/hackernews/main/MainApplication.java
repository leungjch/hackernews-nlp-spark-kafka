package com.hackernews.main;

import java.io.IOException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.google.gson.Gson;
import com.hackernews.api.HnApi;

@SpringBootApplication
public class MainApplication {

	public static void main(String[] args) throws IOException, InterruptedException {

		HnApi hnApi = new HnApi();
		// String data = hnApi.getById(8863);

		for (int i = 11619851; i > 0; i--) {
			String data = hnApi.getById(i);
			if (i % 1000 == 0) {
				System.out.println(i);
			}
			Thread.sleep(10);
		}
		Thread.sleep(1000 * 60);
	}
}
