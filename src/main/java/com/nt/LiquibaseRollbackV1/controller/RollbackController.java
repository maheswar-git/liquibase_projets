package com.nt.LiquibaseRollbackV1.controller;

import com.nt.LiquibaseRollbackV1.service.LiquibaseService;
import liquibase.exception.LiquibaseException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/liquibase")
public class RollbackController {

    private final LiquibaseService liquibaseService;

    public RollbackController(LiquibaseService liquibaseService) {
        this.liquibaseService = liquibaseService;
    }

    /**
     * Rollback to a tag. JSON body: {"tag":"v1"}
     */
    @PostMapping("/rollback-to-tag")
    public ResponseEntity<?> rollbackToTag(@RequestBody Map<String, String> body) {
        String tag = body.get("tag");
        if (tag == null || tag.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "tag is required"));
        }
        try {
            liquibaseService.rollbackToTag(tag);
            return ResponseEntity.ok(Map.of("status", "rolled back to tag", "tag", tag));
        } catch (LiquibaseException e) {
            return ResponseEntity.status(500).body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Optional: create a tag programmatically: {"tag":"v2"}
     */
//    @PostMapping("/tag")
//    public ResponseEntity<?> tagDatabase(@RequestBody Map<String,String> body) {
//        String tag = body.get("tag");
//        if (tag == null || tag.isBlank()) {
//            return ResponseEntity.badRequest().body(Map.of("error", "tag is required"));
//        }
//        try {
//            liquibaseService.tagDatabase(tag);
//            return ResponseEntity.ok(Map.of("status", "tag created", "tag", tag));
//        } catch (LiquibaseException e) {
//            return ResponseEntity.status(500).body(Map.of("error", e.getMessage()));
//        }
//    }
}
