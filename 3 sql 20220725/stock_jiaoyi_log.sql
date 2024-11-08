CREATE DATABASE  IF NOT EXISTS `stock` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `stock`;
-- MySQL dump 10.13  Distrib 8.0.26, for Linux (x86_64)
--
-- Host: 127.0.0.1    Database: stock
-- ------------------------------------------------------
-- Server version	8.0.26

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `jiaoyi_log`
--

DROP TABLE IF EXISTS `jiaoyi_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `jiaoyi_log` (
  `ts_code` varchar(12) COLLATE utf8_bin DEFAULT NULL,
  `buyprice` double DEFAULT NULL,
  `buytime` text COLLATE utf8_bin,
  `sellprice` text COLLATE utf8_bin,
  `selltime` text COLLATE utf8_bin,
  `shouyi` text COLLATE utf8_bin,
  `selltype` double DEFAULT NULL,
  `state` text COLLATE utf8_bin,
  `id` int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=22 DEFAULT CHARSET=utf8mb3 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jiaoyi_log`
--

LOCK TABLES `jiaoyi_log` WRITE;
/*!40000 ALTER TABLE `jiaoyi_log` DISABLE KEYS */;
INSERT INTO `jiaoyi_log` VALUES ('000851',6.77,'20210914','6.52','20211010 09:39:50',NULL,3,'selled',1),('002195',3.207,'20211115','3.07','20211116 10:33:44',NULL,3,'selled',2),('002475',31.68,'20211127','31.45','20211128 09:37:05',NULL,3,'selled',3),('002501',1.6,'20211122','1.53','20211123 09:30:10',NULL,3,'selled',4),('002792',18.93,'20211013','21.45','20211016 09:56:14',NULL,2,'selled',5),('300063',7.45,'20211127','7.4','20211120 09:30:30',NULL,3,'selled',6),('000777',2.11,'20211127','0','','0',0,'chiyou',7),('300131',7.97,'20211130','7.45','20211201 09:30:00','0',3,'selled',8),('300131',7.97,'20211130','7.45','20211201 09:30:00','0',3,'selled',9),('300131',7.97,'20211130','7.45','20211201 09:30:00','0',3,'selled',10),('300131',7.97,'20211130','7.45','20211201 09:30:00','0',3,'selled',11),('300131',7.97,'20211130','7.45','20211201 09:30:00','0',3,'selled',12),('002031',4.02,'20211130','0','','0',0,'selled',13),('002031',4.02,'20211130','0','','0',0,'chiyou',14),('002031',4.02,'20211130','0','','0',0,'chiyou',15),('002031',4.02,'20211130','0','','0',0,'chiyou',16),('002031',4.02,'20211130','0','','0',0,'chiyou',17),('002031',4.02,'20211130','0','','0',0,'chiyou',18),('002031',4.02,'20211130','0','','0',0,'chiyou',19),('002031',4.02,'20211130','0','','0',0,'chiyou',20),('300315',5.64,'20211204','0','','0',0,'chiyou',21);
/*!40000 ALTER TABLE `jiaoyi_log` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed 3 on 2021-12-04 22:12:33
