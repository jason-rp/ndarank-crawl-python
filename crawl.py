# -*- coding: utf-8 -*-
import re
import requests
import threading
from Service.Class_Thread import Threader

class OrderedHeaders(object):

    def __init__(self, headers):
        self.headers = headers

    def items(self):
        return iter(self.headers)

class Crawl_Request(object):
    def __init__(self):
        self.session = requests.Session()
        self.UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
        self.session.headers['User-Agent'] = self.UserAgent
        self.verifed = False
        self.allow_redirects = False
        self.timeout = 30
        self.arrProfile = []
        self.arrCardProfile = []
        self.lock_AddCard = threading.Lock()
        self.lock_AddProfile = threading.Lock()
    def findCardProfile(self, textHTML):
        for (url) in re.findall(r'<span class="profile-card">.*?<a href="(.*?)" title=".*?"', textHTML, re.S):
            idProfile = re.findall(r'timnguoiyeu.com.vn/user/index/(\d{1,10})$', url)[0]
            if str(idProfile).isdigit():
                with self.lock_AddCard:
                    self.arrCardProfile.append((idProfile, "http://timnguoiyeu.com.vn/user/index/%s" %(str(idProfile))))
    def findProfile(self, idProfile, textHTML):
        name = "null"
        lives = "null"
        gender = "null"
        age = "null"
        height = "null"
        education = "null"
        job = "null"
        income = "null"
        marriage = "null"
        staying = "null"
        child = "null"
        zodiac = "null"
        target = "null"
        condition = "null"
        form = "null"
        introduce = "null"

        formProfile = re.findall(r'</h2>(.*?)<div class="clear"></div><div class="clear"></div>', textHTML, re.S)[0]
        for (key, value) in re.findall(r'<div class="muc">(.*?):(.*?)$', formProfile, re.M):
            key = re.sub("<.*?>", "", key).strip()
            value = re.sub("<.*?>", "", value).strip()
            if "Xem Số" in value and "Phone" in key:
                continue
            key = key.replace("♡ ", "")
            if 'Họ và tên' in key:
                name = value
            if 'Sống tại' in key:
                lives = value
            if 'Giới tính' in key:
                gender = value
            if 'Độ tuổi' in key:
                age = value
            if 'Chiều cao' in key:
                height = value
            if 'Học vấn' in key:
                education = value
            if 'Nghề nghiệp' in key:
                job = value
            if 'Thu nhập' in key:
                income = value
            if 'Hôn nhân' in key:
                marriage = value
            if 'Chỗ ở' in key:
                staying = value
            if 'Con cái' in key:
                child = value
            if 'Cung hoàng đạo' in key:
                zodiac = value
            if 'Mục tiêu' in key:
                target = value
            if 'Trạng thái' in key:
                condition = value
            if 'Hình thức' in key:
                form = value
            if 'Giới thiệu' in key:
                introduce = value
        pass
        with self.lock_AddProfile:
            self.arrProfile.append((idProfile, name.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), lives.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), gender.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), age.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), height.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), education.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), job.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), income.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), marriage.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), staying.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), child.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), zodiac.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), target.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), condition.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), form.replace(', ', ' ').replace(',','').replace("'",'').replace('"',''), introduce.replace(', ', ' ').replace(',','').replace("'",'').replace('"','')))
    def urlCardProfile(self, url):
        try:
            headers = OrderedHeaders((
                ('Host', 'timnguoiyeu.com.vn'),
                ('Connection', 'keep-alive'),
                ('Cache-Control', 'no-cache'),
                ('Upgrade-Insecure-Requests', '1'),
                ('User-Agent', self.UserAgent),
                ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'),
                ('Pragma', 'no-cache'),
                ('Referer', 'http://timnguoiyeu.com.vn/'),
                ('Accept-Encoding', 'gzip, deflate'),
                ('Accept-Language', 'en-US,en;q=0.9,vi;q=0.8'),
            ))
        except:
            return self.urlCardProfile(url)
        try:
            response = self.session.get(url, headers=headers, verify=self.verifed, allow_redirects=self.allow_redirects, timeout=self.timeout)
        except:
            return self.urlCardProfile(url)
        pass
        if response.status_code != 200:
            return self.urlCardProfile(url)
        pass

        return response.content
        pass
    def urlProfile(self, url):
        try:
            headers = OrderedHeaders((
                ('Host', 'timnguoiyeu.com.vn'),
                ('Connection', 'keep-alive'),
                ('Cache-Control', 'no-cache'),
                ('Upgrade-Insecure-Requests', '1'),
                ('User-Agent', self.UserAgent),
                ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'),
                ('Pragma', 'no-cache'),
                ('Referer', 'http://timnguoiyeu.com.vn/'),
                ('Accept-Encoding', 'gzip, deflate'),
                ('Accept-Language', 'en-US,en;q=0.9,vi;q=0.8'),
            ))
        except:
            return self.urlProfile(url)
        try:
            response = self.session.get(url, headers=headers, verify=self.verifed, allow_redirects=self.allow_redirects, timeout=self.timeout)
        except:
            return self.urlProfile(url)
        pass
        if response.status_code != 200:
            return self.urlProfile(url)
        pass

        return response.content
    def getMaxPage(self):
        maxPage = re.findall(r'<b class="ipage">(.*?)</b>', self.urlCardProfile('http://timnguoiyeu.com.vn/trang-100000.html'))
        maxPage = maxPage[0] if len(maxPage) == 1 else 5000
        maxPage = maxPage if str(maxPage).isdigit() else 5000
        return int(maxPage)

Crawl_Manager = Crawl_Request()

#maxPage = Crawl_Manager.getMaxPage()
maxPage = 2

print maxPage

threader= Threader(maxPage)

def Crawal_Card_Profile_Run(i):
    Crawl_Manager.findCardProfile(Crawl_Manager.urlCardProfile('http://timnguoiyeu.com.vn/trang-{i}.html'.format(i=i)))
def Crawal_Profile_Run(idProfile, urlProfile):
    Crawl_Manager.findProfile(idProfile, Crawl_Manager.urlProfile(urlProfile))

if maxPage > 1:
    for i in range(1, maxPage):
        threader.put(Crawal_Card_Profile_Run,[str(i)])
    pass
    threader.finish_all()
else:
    Crawl_Manager.findCardProfile(Crawl_Manager.urlCardProfile('http://timnguoiyeu.com.vn/'))
pass
threader = Threader(len(Crawl_Manager.arrCardProfile))

for (idProfile, urlProfile) in Crawl_Manager.arrCardProfile:
    threader.put(Crawal_Profile_Run,[str(idProfile), str(urlProfile)])
pass
threader.finish_all()

raw_json = []
[raw_json.append( "(" + '{id}, {name}, {lives}, {gender}, {age}, {height}, {education}, {job}, {income}, {marriage}, {staying}, {child}, {zodiac}, {target}, {condition}, {form}, {introduce})'.format(id=idProfile,
name="'" + name + "'" if name != "null" else "null",
lives="'" + lives + "'" if lives != "null" else "null",
gender="'" + gender + "'" if gender != "null" else "null",
age="'" + age + "'" if age != "null" else "null",
height="'" + height + "'" if height != "null" else "null",
education="'" + education + "'" if education != "null" else "null",
job="'" + job + "'" if job != "null" else "null",
income="'" + income + "'" if income != "null" else "null",
marriage="'" + marriage + "'" if marriage != "null" else "null",
staying="'" + staying + "'" if staying != "null" else "null",
child="'" + child + "'" if child != "null" else "null",
zodiac="'" + zodiac + "'" if zodiac != "null" else "null",
target="'" + target + "'" if target != "null" else "null",
condition="'" + condition + "'" if condition != "null" else "null",
form="'" + form + "'" if form != "null" else "null",
introduce="'" + introduce + "'" if introduce != "null" else "null")) for (idProfile, name, lives, gender, age, height, education, job, income, marriage, staying, child, zodiac, target, condition, form, introduce) in Crawl_Manager.arrProfile]

f = open("Exported.sql", "w")
f.write("CREATE TABLE IF NOT EXISTS `data1` (\n")
f.write("`id` INT,\n")
f.write("`name` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`lives` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`gender` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`age` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`height` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`education` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`job` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`income` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`marriage` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`staying` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`child` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`zodiac` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`target` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`condition` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`form` VARCHAR(65535) CHARACTER SET utf8,\n")
f.write("`introduce` VARCHAR(65535) CHARACTER SET utf8\n")
f.write(");\n")
f.write("INSERT INTO `data1` VALUES\n")
f.write(",\n".join(raw_json))
f.close()