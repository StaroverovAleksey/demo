const fs = require('fs');
const md5 = require("md5");
const request = require("request-promise");
const readline = require('readline');
const slesh = process.env.OS === 'Windows_NT' ? '\\' : '/';
let options;
if (process.argv.length > 2) {
    options = require(process.argv[2]);
} else {
    throw ('Необходимо указать обязательный параметр, содержащий путь к файлу options.json');
}

class Request {
    constructor() {
        this.sessionId = '';
        this.URL = 'https://app.yaenergetik.ru/api?v2';
    }
    options = (method, params) => {
        return {
            method: 'POST',
            uri: this.URL,
            headers: this.sessionId ? {
                'content-type': 'application/json; charset=utf-8',
                'X-Session-Id': this.sessionId
            } : {
                'content-type': 'application/json; charset=utf-8',
            },
            body: JSON.stringify({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": 1
            })
        }
    }

    authorization = (params) => {
        return request(this.options('auth.login', params))
            .then((response) => {
                this.sessionId = JSON.parse(response).result
            })
            .catch((err) => {
                console.log(err);
            })
    }

    getData = (params) => {
        return request(this.options('reading.list', params))
            .catch((err) => {
                console.log(err);
            })
    }

    getPower = (params) => {
        return request(this.options('powerProfile.data', params))
            .catch((err) => {
                console.log(err);
            })
    }
}

class Parser extends Request {
    constructor() {
        super();
        this.year = parseInt(options['startDate'].split('.')[2]);
        this.month = parseInt(options['startDate'].split('.')[1]);
        this.stopDate = [new Date().getFullYear(), new Date().getMonth() + 1]
        this.user = options['yaenergetik']['user'];
        this.apiKey = options['yaenergetik']['apiKey'];
        this.meters = options['meters'];
        this.meter = this.meters[0];
        this.path = options['path'];
        this.countNewFiles = 0;
        this.countDate = `${options['startDate'].split('.')[1]}.${options['startDate'].split('.')[2]}`;
        this.filesPerSecond = 0;
        this.rid = 0;
        this.page = 1;
        this.successMeters = [];
        this.paramsAuth = {
            "mode": "server",
            "user": this.user,
            "apiKey": this.apiKey
        };
        this.endWork = false;
        this.startTime = new Date().getTime();
    }

    /***Старт парсинга*/
    start = async () => {
        this.writeToConsole();
        for (const meter of this.meters) {
            this.meter = meter;
            await this.initializationParams(meter);
            await this.checkPath();
            await this.parseOneMeter();
            this.successMeters.push({
                imei: this.meter['imei']
                , rid: this.rid
                , name: this.meter['name']
            })
            this.year = parseInt(options['startDate'].split('.')[2]);
            this.month = parseInt(options['startDate'].split('.')[1]);
            this.rid = 0;
        }
        this.endWork = true;
    }

    /***Инициализация объектов запросов. Происходит при кажлом сдвиге месяца и при смене счетчика*/
    initializationParams = () => {
        this.paramsData = {
            "meter": parseInt(this.meter['yaenergetik_id']),
            "include": ["zones"],
            "period": {
                "type": "month",
                "value": `${this.year}-${this.month}`
            },
            "mode": "all",
            "sort": "asc"
        }
        this.paramsPower = {
            "meter": parseInt(this.meter['yaenergetik_id']),
            "include": ["reactive", "reverse"],
            "period": {
                "type": "month",
                "value": `${this.year}-${this.month}`
            }
        }
    }

    /***Создает директории для записи данных. Если директории уже существуют, то рекурсивно удаляет их со всем содержимым и создает заново*/
    checkPath = async () => {
        const pathData = this.path + slesh + this.meter.imei.toString() + slesh + 'data';
        const pathPower = this.path + slesh + this.meter.imei.toString() + slesh + 'power';

        try {
            await fs.rmdirSync(pathData, {recursive: true});
            await fs.mkdirSync(pathData);
        } catch (err) {
            await fs.mkdirSync(pathData, {recursive: true});
        }

        try {
            await fs.rmdirSync(pathPower, {recursive: true});
            await fs.mkdirSync(pathPower);
        } catch (err) {
            await fs.mkdirSync(pathPower, {recursive: true});
        }
    }

    parseOneMeter = async () => {

        while (this.year <= this.stopDate[0] && this.month <= this.stopDate[1]) {
            await this.parseOneMonthData();
            await this.dateShifter();
        }

        this.year = parseInt(options['startDate'].split('.')[2]);
        this.month = parseInt(options['startDate'].split('.')[1]);
        await this.initializationParams();

        while (this.year <= this.stopDate[0] && this.month <= this.stopDate[1]) {
            await this.parseOneMonthPower();
            await this.dateShifter();
        }
    }

    parseOneMonthData = async () => {
        await this.getData(this.paramsData).then( async (response) => {
            const result = JSON.parse(response);
            if (typeof result['error'] === "undefined") {
                await this.writeData(result.result);
            } else if (result['error'].message === 'Too many data requested') {
                await this.parseDataByPage(this.paramsData);
            } else {
                throw (result['error']);
            }
        });
    }

    parseDataByPage = async () => {
        this.paramsData.page = {size: 1000, index: this.page};
        await this.getData(this.paramsData).then( async (response) => {
            const result = JSON.parse(response);
            if (typeof result['error'] === "undefined") {
                await this.writeData(result.result.values);
                if(result.result.pageCount > this.page) {
                    this.page ++
                    await this.parseDataByPage();
                } else {
                    this.page = 1;
                }
            } else {
                throw (result['error']);
            }
        });
    }

    parseOneMonthPower = async () => {
        await this.getPower(this.paramsPower).then( async (response) => {
            const result = JSON.parse(response);
            if (typeof result['error'] === "undefined") {
                await this.writePower(result.result);
            } else {
                throw (result['error']);
            }
        });
    }

    writeData = async (result) => {
        let day = '';
        for (const sample of result) {
            this.rid = ++this.rid;
            if (sample.date.split('T')[0] !== day) {
                day = sample.date.split('T')[0];
            }
            const ts = Date.parse(sample.date);
            const dateUTC = new Date(ts);

            const pathData = this.path + slesh + this.meter.imei.toString() + slesh + 'data'
                + `${slesh}${dateUTC.getUTCFullYear()}`
                + `${slesh}${(dateUTC.getUTCMonth() + 1).toString().padStart(2, '0')}`
                + `${slesh}${dateUTC.getUTCDate().toString().padStart(2, '0')}`;
            const data = await this.formatData(sample, this.rid, dateUTC);
            await fs.mkdirSync(pathData, {recursive: true});
            await fs.writeFileSync(pathData + `${slesh}${this.rid}.json`, data);
            this.countNewFiles = ++this.countNewFiles;
            this.countDate = `${sample.date.split('-')[1]}.${sample.date.split('-')[0]}`
        }
    }

    formatData = async (data, count, dateUTC) => {
        const date = dateUTC.getUTCDate().toString().padStart(2, '0') +
            '.' + (dateUTC.getUTCMonth() + 1).toString().padStart(2, '0') +
            '.' + dateUTC.getUTCFullYear() + ' ' + dateUTC.getUTCHours().toString().padStart(2, '0') +
            ':' + dateUTC.getUTCMinutes().toString().padStart(2, '0') +
            ':' + dateUTC.getUTCSeconds().toString().padStart(2, '0');

        let qwerty = JSON.stringify({
            rid: count,
            imei: parseInt(this.meter.imei),
            date_and_time: date,
            date_and_time_meter: date,
            energy: {
                a_day: data.zones[0],
                a_night: data.zones[1],
                a_sum: data.value
            }
        }, null, '\t').slice(0, -2) + ',\n\t"';
        return qwerty + `md5": "${md5(qwerty)}"\n}`;
    }

    writePower = async (result) => {
        let day = '';
        for (const sample of result.data) {
            this.rid = ++this.rid;
            if (sample[0].split('T')[0] !== day) {
                day = sample[0].split('T')[0];
            }

            const pathData = this.path + slesh + this.meter.imei.toString() + slesh + 'power'
                + `${slesh}${day.split('-')[0]}`
                + `${slesh}${day.split('-')[1]}`
                + `${slesh}${day.split('-')[2]}`;
            const data = await this.formatPower(sample, this.rid);
            await fs.mkdirSync(pathData, {recursive: true});
            await fs.writeFileSync(pathData + `${slesh}${this.rid}.json`, data);
            this.countNewFiles = ++this.countNewFiles;
            this.countDate = `${sample[0].split('-')[1]}.${sample[0].split('-')[0]}`;
        }
    }

    formatDate = (data) => {
        const date = data.split('T')[0].split('-').reverse();
        const time = data.split('T')[1].split('+')[0];
        return date.join('.') + ' ' + time;
    }

    formatPower = async (data, count) => {
        let qwerty = JSON.stringify({
            rid: count,
            imei: parseInt(this.meter.imei),
            date_and_time: this.formatDate(data[0]),
            date_and_time_meter: this.formatDate(data[0]),
            energy: {
                a_power: [{
                    date: this.formatDate(data[0]),
                    power: data[1]
                }],
                ao_power: [{
                    date: this.formatDate(data[0]),
                    power: data[2]
                }],
                r_power: [{
                    date: this.formatDate(data[0]),
                    power: data[3]
                }],
                ro_power: [{
                    date: this.formatDate(data[0]),
                    power: data[4]
                }],
            }
        }, null, '\t').slice(0, -2) + ',\n\t"';
        return qwerty + `md5": "${md5(qwerty)}"\n}`;
    }

    /***Сдвигает this.year и this.month на 1 месяц в прошлое*/
    dateShifter = () => {
        if (this.month === 12) {
            this.year = ++this.year;
            this.month = 1;
        } else {
            this.month = ++this.month;
        }
        this.initializationParams();
    }

    formatConsole = () => {
        this.filesPerSecond = Math.round(this.countNewFiles / ((new Date().getTime() - this.startTime) / 1000));

        const consoleArray = [
            `Обрабатываются данные по счетчику:`
            , `ID.............. ${this.meter['yaenergetik_id']}`
            , `IMEI............ ${this.meter['imei']}`
            , `За дату......... ${this.countDate}`
            , `Создано файлов:`
            , `Всего........... ${this.countNewFiles}`
            , `За секунду...... ${this.filesPerSecond}`
            , ``
        ];

        this.successMeters.forEach((value) => {
            consoleArray.push(`${value.imei}, ${value.name}, создано файлов: ${value.rid}, \x1b[32m success \x1b[0m`);
        });
        consoleArray.forEach((value, index) => {
            readline.cursorTo(process.stdout, 0, index + 1);
            process.stdout.write(value + '        ');
            if (index === consoleArray.length - 1) {
                readline.cursorTo(process.stdout, 0, index + 2);
                process.stdout.write('\t');
            }
        });

        if(this.endWork) {
            clearInterval(this.writeCounter);
        }
    }

    writeToConsole = () => {
        process.stdout.write('\x1Bc');
        this.formatConsole();
        this.writeCounter = setInterval(() => this.formatConsole(), 2000);
    }

}

const parser = new Parser();
parser.authorization(parser.paramsAuth)
    .then(parser.start)
