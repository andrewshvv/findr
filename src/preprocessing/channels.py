import os

from telethon.tl.types import InputPeerChannel

test_channel = {
    'access_hash': 469327645547328819,
    'channel_id': 1639166908,
    'name': "Test channel"
}

assert (os.getenv("ENV") is not None)

if os.getenv("ENV") == "PROD":
    assert (os.getenv("PROD_TRANSIENT_CHANNEL_ID") is not None)
    assert (os.getenv("PROD_TRANSIENT_CHANNEL_HASH") is not None)

    TRANSIENT_CHANNEL = InputPeerChannel(
        channel_id=int(os.getenv("PROD_TRANSIENT_CHANNEL_ID")),
        access_hash=int(os.getenv("PROD_TRANSIENT_CHANNEL_HASH"))
    )
elif os.getenv("ENV") in ["DEV", "TEST"]:
    assert (os.getenv("DEV_TRANSIENT_CHANNEL_ID") is not None)
    assert (os.getenv("DEV_TRANSIENT_CHANNEL_HASH") is not None)

    TRANSIENT_CHANNEL = InputPeerChannel(
        channel_id=int(os.getenv("DEV_TRANSIENT_CHANNEL_ID")),
        access_hash=int(os.getenv("DEV_TRANSIENT_CHANNEL_HASH"))
    )
else:
    raise NotImplementedError(f"Unknown env: {os.getenv('ENV')}")

ALL_CHANNELS = [
    {
        'access_hash': 2976770772219907335,
        'channel_id': 1164672298,
        'name': "Remote Junior"
    },
    {'access_hash': 8428875027805792449,
     'channel_id': 1389339613,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Job for Mobile: iOS, Android, React Native'
     },
    {'access_hash': 541831534101569748,
     'channel_id': 1193527943,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'React Job | JavaScript | Вакансии'
     },
    {'access_hash': -898868513418622808,
     'channel_id': 1158822652,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Job for Sales & BizDev'},
    {'access_hash': 9076778684002781890,
     'channel_id': 1381822968,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Job for Python'},
    {'access_hash': 7919687926212840303,
     'channel_id': 1121739665,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'IT / Tech jobs',
     'stop_list': {
         """Откликнуться""": "sentence",
         """Также укажите, что узнали о вакансии в телеграм-канале""": "sentence",
     }
     },
    {'access_hash': 903953412204021494,
     'channel_id': 1093073202,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Job for Products and Projects'},
    {'access_hash': 1844987617375418658,
     'channel_id': 1278223896,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Работа для программистов'},
    {'access_hash': -3977871306693611190,
     'channel_id': 1311122978,
     'is_description_behind_link': True,
     'multi_job_per_post': True,
     'name': 'Job for Gamedev'
     },
    {'access_hash': -6024297560067880330,
     'channel_id': 1120288601,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Game Development Jobs'},
    {'access_hash': -322044654691326781,
     'channel_id': 1304726099,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Job for Junior'},
    {'access_hash': 1687002375173859764,
     'channel_id': 1347539956,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Job for Frontend (JavaScript + Node.js) developers'},
    {'access_hash': -5802747999461008919,
     'channel_id': 1213858047,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Job for Sysadmin & DevOps'},
    {'access_hash': -7356759028711771809,
     'channel_id': 1137236002,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Job for Analysts & Data Scientists'},
    {'access_hash': 8485381346591159987,
     'channel_id': 1344577123,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Backend Job Offers',
     'stop_list': {
         """Больше вакансий для""": "sentence"
     }
     },
    {'access_hash': -2361681074873080876,
     'channel_id': 1284685057,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Data Science & Analytics Job Offers',
     'stop_list': {
         """Больше вакансий для""": "sentence"
     }
     },
    {'access_hash': 4831027988192722571,
     'channel_id': 1552358777,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Front-end Job Offers',
     'stop_list': {
         """Больше вакансий для""": "sentence"
     }
     },
    {'access_hash': 241375852089008841,
     'channel_id': 1582627575,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Python Job Offers',
     'stop_list': {
         """Больше вакансий для""": "sentence"
     }
     },
    {'access_hash': -2528447617351018712,
     'channel_id': 1613192375,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'JavaScript Job Offers',
     'stop_list': {
         """Больше вакансий для""": "sentence"
     }
     },
    {'access_hash': -4263815108725822742,
     'channel_id': 1697683423,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Web-Development Job Offers',
     'stop_list': {
         """Больше вакансий для""": "sentence"
     }
     },
    {'access_hash': -7836502298176282540,
     'channel_id': 1720285887,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Mobile App Development Job Offers',
     'stop_list': {
         """Больше вакансий для""": "sentence"
     }
     },
    {'access_hash': 868338523758586103,
     'channel_id': 1778222868,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'C#/.Net Job Offers',
     'stop_list': {
         """Больше вакансий для""": "sentence"
     }
     },
    {'access_hash': -8824982914942903222,
     'channel_id': 1134745498,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Devops Jobs — вакансии и резюме',
     "stop_list": {
         """Обсуждение вакансии в чате""": "sentence",
         """Публикатор""": "sentence"
     }

     },
    {'access_hash': 8274346033205165479,
     'channel_id': 1336250861,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'IT Jobs | Вакансии в IT'},
    {'access_hash': 7098560528067664368,
     'channel_id': 1399472074,
     'is_description_behind_link': True,
     'multi_job_per_post': True,
     'name': 'Студент Маминой Подруги'},
    {'access_hash': 6538460944693708792,
     'channel_id': 1411007322,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Java Job | Вакансии'},
    {'access_hash': -5490274602456600293,
     'channel_id': 1281962041,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'JVM Jobs'},
    {'access_hash': 4998128328237565150,
     'channel_id': 1091870362,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Работа в ИТ',
     'stop_list': {
         """--\n[< Подпишитесь на канал «Работа в ИТ» >]""": "chunk"
     }
     },
    {'access_hash': -5909329379632871806,
     'channel_id': 1512435004,
     'is_description_behind_link': True,
     'multi_job_per_post': True,
     'name': 'СЕТИ — IT & Digital вакансии'},
    {'access_hash': -2400148711280571894,
     'channel_id': 1442301657,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Вакансии в IT от hh.ru'},
    {'access_hash': 2542445197710390363,
     'channel_id': 1422211563,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'JavaScript Job | Вакансии | Стажировки'},
    {'access_hash': 4573017443947439657,
     'channel_id': 1253965277,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Job for QA'},
    {'access_hash': 4469727030140548976,
     'channel_id': 1141029953,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Remote IT (Inflow)'},
    {'access_hash': 2762659366077032729,
     'channel_id': 1109222536,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Работа в геймдеве 🍖'},
    {'access_hash': -6765321105930919346,
     'channel_id': 1780531805,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'IT Jobs (No Code)'},
    {'access_hash': 3015706059178851542,
     'channel_id': 1165814759,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'YandexTeam нанимает разработчиков'},
    {'access_hash': 7661085506366999415,
     'channel_id': 1292405242,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Python Job | Вакансии | Стажировки'},
    {'access_hash': 5356555690955987946,
     'channel_id': 1212014211,
     'is_description_behind_link': True,
     'multi_job_per_post': True,
     'name': 'C# jobs — вакансии по C#, .NET, Unity',
     'stop_list': {
         """Это #партнерский пост""": "sentence"
     }
     },
    {'access_hash': 7300955172047877354,
     'channel_id': 1458440404,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Web 3.0 Job | Вакансии'},
    {'access_hash': 7616496655744069896,
     'channel_id': 1537669054,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'Golang Job Offers',
     'stop_list': {
         """Больше вакансий для""": "sentence"
     }
     },
    {'access_hash': -7574898543547144928,
     'channel_id': 1662664152,
     'is_description_behind_link': False,
     'multi_job_per_post': False,
     'name': 'HTML Job Offers',
     'stop_list': {
         """Больше вакансий для""": "sentence"
     }
     },
    {
        'access_hash': -4582612528289746852,
        'channel_id': 1780472872,
        'is_description_behind_link': False,
        'multi_job_per_post': False,
        'name': 'C/C++ Job Offers',
        'stop_list': {
            """Больше вакансий для""": "sentence"
        }
    },
    {'access_hash': 5866931421256921156,
     'channel_id': 1447304363,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Jobs Code: IT вакансии'},
    {'access_hash': -1883206108678240761,
     'channel_id': 1459679492,
     'is_description_behind_link': True,
     'multi_job_per_post': False,
     'name': 'Удаленка — IT и Digital'},
    {
        'access_hash': 6073439727096356645,
        'channel_id': 1817966996,
        'is_description_behind_link': False,
        'multi_job_per_post': False,
        'name': 'IT vacancies',
        'stop_list': {
            """┃🧑‍💻Получай больше вакансии """: "chunk",
            """┃и подработок в нашем боте""": "chunk",
            """┃ - @jobprbot""": "chunk",
            """➖➖➖➖➖➖➖➖➖➖➖""": "chunk",
            """Если хотите пожаловаться на вакансию - пишите по контактам в описании канала""": "chunk",
            """——————————""": "chunk",
            """📱 Вакансии для новичков ★""": "chunk",
            """Разместить вакансию""": "chunk"
        }
    }
]


# {'access_hash': -6110833204924972431,
#      'channel_id': 1284368373,
#      'is_description_behind_link': False,
#      'multi_job_per_post': False,
#      'name': 'Р1: Работа. Вакансии 1С.'},

# ]

def get_stop_list(source):
    for channel in ALL_CHANNELS:
        if channel["channel_id"] == int(source.split(":")[1]):
            stop_list = channel.get("stop_list", {})
            if not isinstance(stop_list, dict):
                raise NotImplementedError
            return stop_list

    return {}


if os.getenv("ENV") == "TEST":
    ACTIVE_CHANNELS = [
        (
            InputPeerChannel(channel["channel_id"], channel["access_hash"]),
            channel["name"],
            channel.get("stop_list")
        )
        for channel in [test_channel]
    ]
else:
    ACTIVE_CHANNELS = [
        (
            InputPeerChannel(channel["channel_id"], channel["access_hash"]),
            channel["name"],
            channel.get("stop_list")
        )
        for channel in ALL_CHANNELS
    ]
