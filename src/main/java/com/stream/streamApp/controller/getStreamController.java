package com.stream.streamApp.controller;

import com.stream.streamApp.config.StreamProcessing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class getStreamController {

    @Autowired
    private StreamProcessing streamProcessing;

    @GetMapping("/getstream")
    public void getStream(){
        this.streamProcessing.processStream();

    }

}
