-- MariaDB dump 10.19  Distrib 10.5.23-MariaDB, for Linux (x86_64)
--
-- Host: 127.0.0.1    Database: sync-src
-- ------------------------------------------------------
-- Server version	10.11.2-MariaDB-1:10.11.2+maria~ubu2204

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `tabella`
--

DROP TABLE IF EXISTS `tabella`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tabella` (
  `key` int(11) NOT NULL AUTO_INCREMENT,
  `intero` int(11) DEFAULT NULL,
  `stringa` varchar(100) DEFAULT NULL,
  `timestamp` datetime DEFAULT NULL,
  `blob` longtext DEFAULT NULL,
  `decimale` decimal(8,4) DEFAULT NULL,
  PRIMARY KEY (`key`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tabella`
--

LOCK TABLES `tabella` WRITE;
/*!40000 ALTER TABLE `tabella` DISABLE KEYS */;
INSERT INTO `tabella` VALUES (1,1,'ciao','2024-03-22 15:00:00',NULL,1.5000),(2,2,NULL,NULL,'001c0d70: 17b2 97ea 4d8c 0ea3 ed46 6a6d 7cbf e59b  ....M....Fjm|...\n001c0d80: 435c 44e4 e29c 9fa9 f651 bf6b 6807 a3a4  C\\D......Q.kh...\n001c0d90: 89fc 68b4 4220 b41e c5b1 57ed 02a5 5bb8  ..h.B ....W...[.\n001c0da0: a0fa 533a 2e7d 5b76 15e4 3779 63d5 780f  ..S:.}[v..7yc.x.\n001c0db0: b42c 97ad 2b27 dc2e 6e22 6717 1b05 7872  .,..+\'..n\"g...xr\n001c0dc0: a1ba 60d7 6daa 1975 deab 336a 4092 48fe  ..`.m..u..3j@.H.\n001c0dd0: 3562 f4b0 a721 fb25 f385 81b6 3284 73fa  5b...!.%....2.s.\n001c0de0: 36c6 e651 1d40 2d8d 9626 c000 9343 3c25  6..Q.@-..&...C<%\n001c0df0: ed3c 41b1 f0e8 56e0 093a c586 b827 9dd4  .<A...V..:...\'..\n001c0e00: c6e5 12a9 7aed fc02 ce47 9344 f5cc b230  ....z....G.D...0\n001c0e10: 9706 13f3 7f87 b07b 6a15 c4c7 3391 6ef6  .......{j...3.n.\n001c0e20: c834 a877 6be6 50bb 950a 11a5 0128 b0a0  .4.wk.P......(..\n001c0e30: f155 6d5f e393 da47 54ce c06f 703a f7fa  .Um_...GT..op:..\n001c0e40: aa4b 461b 5390 8a9e 8d48 7d30 7b99 576e  .KF.S....H}0{.Wn\n001c0e50: c35f ebda a3f0 2e4c 9d06 abb3 1ba3 4f4e  ._.....L......ON\n001c0e60: 37b6 35de e3d6 7ec7 36fb 6bb4 79da 8d6c  7.5...~.6.k.y..l\n001c0e70: 4f17 a475 1de8 e430 5cd7 f7d7 43f3 5387  O..u...0\\...C.S.\n001c0e80: 5645 c1df 8bce de02 ea22 c978 7141 ded0  VE.......\".xqA..\n001c0e90: aba0 eb37 98dc 25ec d130 cbe4 e513 5511  ...7..%..0....U.\n001c0ea0: 8dc3 09d8 1afe ed48 3cbc 3c28 e210 6f2f  .......H<.<(..o/\n001c0eb0: 79d0 685a 97f3 82e9 d63c 9b68 139f 4d37  y.hZ.....<.h..M7\n001c0ec0: d9cb ac1a 2799 9d93 3f4f 1fab 1e7b 3e92  ....\'...?O...{>.\n001c0ed0: 451f 04ec 33b7 3c41 004a 3543 d7d3 9710  E...3.<A.J5C....\n001c0ee0: 652a 49ca 54c5 ac7a 8e2a 6ba4 21d7 d2df  e*I.T..z.*k.!...\n001c0ef0: 2bdc c546 2128 f193 2a53 c20a a049 5b3f  +..F!(..*S...I[?\n001c0f00: 39f9 55e4 224c c089 d7fa 5dd3 3861 4312  9.U.\"L....].8aC.\n001c0f10: 448b b16d b193 d2e9 8547 616b 6456 2aec  D..m.....GakdV*.\n001c0f20: 6e3b f26b a0ab c63e 2a97 4969 3e6d cc3c  n;.k...>*.Ii>m.<\n001c0f30: dc48 d23c 69e3 0b4f 1910 bc9b 71dd 00f6  .H.<i..O....q...\n001c0f40: 7150 9da4 8060 520f 0ed5 10ca 87b3 022f  qP...`R......../\n001c0f50: 5790 a3a6 811c b6e8 336b 16c2 09a4 48b7  W.......3k....H.\n001c0f60: 2673 7eca f265 94da 1c27 bf11 a89f c8ab  &s~..e...\'......\n001c0f70: 6072 0eed 2a25 e1ec fde6 0209 b460 5769  `r..*%.......`Wi\n001c0f80: 0d59 86cf 4db7 804b 5482 146a 796d f07c  .Y..M..KT..jym.|\n001c0f90: 0fcf 150d cebf 9901 80a5 abbd cc06 4025  ..............@%\n001c0fa0: 0b4d 8023 64ce e523 335e 3de4 5dd9 9f7b  .M.#d..#3^=.]..{\n001c0fb0: 6e58 3a58 f508 e28b b726 de39 d52f 500e  nX:X.....&.9./P.\n001c0fc0: 976f 9301 1e96 4ce9 2b76 c018 5b7a e66d  .o....L.+v..[z.m\n001c0fd0: 5bd3 79c1 292e 4998 9259 6069 4853 b1e1  [.y.).I..Y`iHS..\n001c0fe0: f87e 4ea1 fc40 b721 d48b 3b41 942b 0d4a  .~N..@.!..;A.+.J\n001c0ff0: 6d38 f536 c185 7f3d e30f 5684 2ffe b44d  m8.6...=..V./..M\n001c1000: 7c55 bd55 3792 feea f802 c534 8003 3a78  |U.U7......4..:x\n001c1010: ddc3 60d0 0b84 8c1b ef12 f6cb 4691 7720  ..`.........F.w \n001c1020: 4622 d743 f174 d70a 62cb fd91 2fd3 abe5  F\".C.t..b.../...\n001c1030: df7a 9bef ca5a d69e d437 2003 cbff c2a3  .z...Z...7 .....\n001c1040: b317 1e1c a58e 17ec 49bf e27c 485b 79d5  ........I..|H[y.',NULL),(3,3,'pippo',NULL,NULL,NULL),(4,4,'con caratteri @ # €',NULL,NULL,6.5000),(5,5,'',NULL,NULL,NULL),(6,NULL,'utf8 ↔',NULL,NULL,NULL);
/*!40000 ALTER TABLE `tabella` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping routines for database 'sync-src'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-03-26 17:27:27
