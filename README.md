# ภาพรวมของบริการ (Service Overview)
## Disaster Scraping Service

## Service Owner
นางสาว ชัญญานุช   อิ่มเกิด    รหัสนักศึกษา 6609650277   ภาคพิเศษ

## Service Purpose
Disaster Scraping Service เป็นบริการที่ทำหน้าที่รวบรวมข้อมูลการแจ้งเตือนสภาพอากาศและภัยพิบัติจากเพจ Facebook กรมอุตุนิยมวิทยาโดยอัตโนมัติผ่านเทคนิค web scraping เพื่อใช้เป็นแหล่งข้อมูลตั้งต้นสำหรับการตรวจจับเหตุการณ์ภัยพิบัติในระบบตอบสนองภัยพิบัติ
บริการนี้ช่วยแปลงข้อมูลจากหน้าเว็บที่ไม่มี API ให้เป็นข้อมูลเชิงโครงสร้าง (structured data) และเผยแพร่เป็น event หรือ API ให้บริการอื่น เช่น Incident Service สามารถนำไปใช้งานต่อได้

## Pain Point ที่แก้ไข
ในปัจจุบัน การแจ้งเตือนภัยพิบัติมักอยู่ในรูปแบบ
  - หน้าเว็บประกาศ
  - ข่าวเตือนภัย
  - ข้อความที่ไม่มี API

ทำให้ระบบอัตโนมัติไม่สามารถนำข้อมูลไปใช้งานได้โดยตรง และต้องอาศัยการติดตามด้วยมนุษย์ ซึ่งมีความล่าช้า
บริการนี้จึงช่วย
  - ดึงข้อมูลประกาศเตือนภัยแบบอัตโนมัติ
  - ตรวจจับเหตุการณ์ใหม่ได้เร็วขึ้น
  - ลด Manual monitoring
  - ทำให้ระบบ disaster response เป็น real-time มากขึ้น

## Target Users
  - Incident Management Service
  - Dashboard แจ้งเตือนภัยพิบัติ
    
       บริการนี้เป็น backend data ingestion service ไม่ได้ให้ผู้ใช้งานทั่วไปเรียกโดยตรง

## Service Boundary
  In-scope Responsibilities (สิ่งที่บริการนี้รับผิดชอบ)
  - ทำ web scraping จาก Website กรมอุตุนิยมวิทยา
  - ตรวจจับประกาศเตือนภัยใหม่
  - แปลงข้อมูลจาก HTML เป็น structured data
  - กรอง duplicate announcements
  - ส่งข้อมูลออกเป็น event หรือ API

  Out-of-scope / Not Responsible For (ไม่รับผิดชอบ)
  - การจัดการศูนย์พักพิง
  - การวิเคราะห์ผลกระทบภัยพิบัติ
  - การแจ้งเตือนผู้ใช้งาน

## Autonomy / Decision Logic
  บริการมีความเป็นอิสระในการตัดสินใจเกี่ยวกับ
  - การตรวจจับว่า announcement ใดเป็นข้อมูลใหม่
  - การ normalize ข้อมูล
  - การตัดสินใจ publish event
  - ตรรกะการตัดสินใจอิงจาก
  - announcement title
  - timestamp
  - content keyword

บริการสามารถตัดสินใจได้เองภายใต้ business rules ที่กำหนด โดยไม่ต้องรอ/ต้องรอการอนุมัติจากมนุษย์ในกรณีปกติ

## Owned Data
1. Scraped Announcement Data
ข้อมูลประกาศที่ดึงมาจาก Website Page หรือแหล่งต้นทางในรูปแบบที่ยังใกล้เคียงต้นฉบับ เช่น ชื่อโพสต์ เนื้อหา วันที่โพสต์ ลิงก์ เป็นต้น ข้อมูลส่วนนี้ถูกเก็บไว้เพื่อใช้ในการตรวจสอบย้อนหลัง (audit) และเพื่อให้สามารถ re-process ได้ในกรณีที่ต้องปรับปรุง logic การวิเคราะห์ในอนาคต
2. Normalized Disaster Event Draft
ข้อมูลที่ผ่านกระบวนการแปลง (normalization) จากข้อความดิบให้เป็น structured format เช่น eventType, severity, publishedAt และ affectedArea ข้อมูลส่วนนี้เป็นผลลัพธ์จาก business rules ภายใน service และเป็นข้อมูลที่พร้อมส่งต่อให้ Incident Service หรือระบบอื่นใช้งาน
3. Scraping Metadata
ข้อมูลประกอบกระบวนการ scraping เช่น เวลาในการดึงข้อมูล (scrapedAt), แหล่งที่มา (sourceUrl), response status และ content hash เพื่อใช้ในการติดตามประสิทธิภาพของระบบ ตรวจสอบปัญหา (debugging) และรองรับการตรวจสอบย้อนหลัง
4. Deduplication State
ข้อมูลที่ใช้ป้องกันการส่ง event ซ้ำ เช่น hash ของเนื้อหา, postId หรือ eventId โดยจะถูกใช้ตรวจสอบก่อน publish event ทุกครั้ง เพื่อรองรับรูปแบบการส่งข้อมูลแบบ at-least-once และป้องกันการสร้าง incident ซ้ำในระบบ downstream

## Linked Data (Reference Only)
  ระบบจะตรวจสอบ incident จาก Incident Service โดย Incident Service ได้ข้อมูลจาก Web scraping กรมอุตุนิยมวิทยา เช่น ประกาศเตือนพายุ ประกาศฝนตกหนัก แจ้งเตือนน้ำท่วม

## Non-Functional Requirements
  - ใช้ at-least-once event delivery
  - ต้องมี idempotency ในการ publish event
  - Retry scraping หาก request ล้มเหลว (ไม่เกิน 3 ครั้ง)
  - ระบบต้องไม่ crash เมื่อ HTML เปลี่ยนรูปแบบ
  - ต้องมี timeout และ error handling
  - ต้องเคารพ rate limit ของเว็บไซต์ต้นทาง


Link GitHub: https://github.com/ChanyanuchImk/cs366_Microservice_0277.git
