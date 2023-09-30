import pathlib
import sqlite3
import argparse
import os
import hashlib
from xml.dom import minidom
from pprint import pprint

#{{{1 minidom setup

def _elem_inplace_addition(self,other):
    self.appendChild(other)
    return self

def _elem_textnode(self,text):
    textnode = self.ownerDocument.createTextNode(text)
    self.appendChild(textnode)
    return self

def _elem_set_attributes_from_tuple(self,*args):
    for k,v in args:
        self.setAttribute(k,str(v))
    return self

minidom.Element.__iadd__ = _elem_inplace_addition
minidom.Element.txt = _elem_textnode
minidom.Element.attrt = _elem_set_attributes_from_tuple
minidom.Element.__str__ = lambda s:s.toprettyxml().strip()

#}}}1

DEFAULTPATH = pathlib.Path("D:\zSamples")
DEFAULTDBPATH = pathlib.Path("_filesdb.db")
BUF_SIZE = 65536
DEFAULT_HTML_OUTPUT = pathlib.Path("dups.html")

args = argparse.ArgumentParser()
args.add_argument("--path",type=pathlib.Path,default=DEFAULTPATH)
args.add_argument("--dbpath",type=pathlib.Path,default=DEFAULTDBPATH)
args.add_argument("--html_output",type=pathlib.Path,default=DEFAULT_HTML_OUTPUT)
args.add_argument("--scan",action="store_true")
args.add_argument("--ext",action="append")
args.add_argument("--dump",action="store_true")
args.add_argument("--dups",action="store_true")
args.add_argument("--analyze",action="store_true")

ns = args.parse_args()
print("argument namespace:",ns)

def mkhash(path):
    m = hashlib.sha256()
    f = open(path,"rb")
    while True:
        data = f.read(BUF_SIZE)
        if not data:
            break
        m.update(data)
    return m.hexdigest()


class FilesLibrarian(sqlite3.Connection):
    ddl = """
    pragma foreign_keys=1;
    pragma recursive_triggers=1;

    create table if not exists files(
    id integer primary key,
    path text,
    name text,
    hash text,
    unique (path) on conflict replace);

    create table if not exists hashes as select distinct hash from files;

    create table if not exists dupcounts(
    id integer primary key,
    hash_id integer,
    ct integer);
    """
    @property
    def cu(self):
        cu = self.cursor()
        cu.row_factory = lambda c,r:r[0]
        return cu
    def __init__(self,name,**kwargs):
        super().__init__(name,**kwargs)
        print("init db:",name)
        self.executescript(self.ddl)
        self.commit()


class Library:
    _handle = None
    @property
    def cx(self):
        if not self._handle:
            self._handle = sqlite3.connect(
                ns.dbpath,
                factory=FilesLibrarian)
        return self._handle
    def populate_files_from(self,path,extensions=None):
        print("path:",path)
        print("extensions:",extensions)
        exts = set()
        if extensions:
            for ext in extensions:
                t = ext
                if not ext.startswith("."):
                    t = "."+ext
                exts.add(t)
        print("exts:",exts)
        for r,ds,fs in os.walk(path):
            r = pathlib.Path(r)
            for f in fs:
                p = r / f
                if not extensions or p.suffix in exts:
                    self.cx.execute(
                        "insert into files (path,name,hash) values (?,?,?)",
                        (str(p),p.name,mkhash(p)))
                    print("p:",p)
        self.cx.commit()
    def analyze(self):
        for hash_id,fhash in self.cx.execute("select rowid,hash from hashes"):
            print("fhash:",hash_id,fhash,end=" ")
            ct = self.cx.cu.execute("select count() from files where hash=?",(fhash,)).fetchone()
            print("ct:",ct)
            if ct > 1:
                self.cx.execute("insert into dupcounts (hash_id,ct) values (?,?)",
                                (hash_id,ct))
                print("ct:",ct)
        self.cx.commit()
    def list_dups(self):
        doc = minidom.Document()
        elem = doc.createElement
        root = elem("html")
        body = elem("body")
        root += body
        ol = elem("ol")
        body += ol
        for hash_id in self.cx.cu.execute("select hash_id from dupcounts order by ct"):
            print("hash_id:",hash_id)
            li = elem("li")
            ol += li
            h4 = elem("h4")
            li += h4
            h4.txt(str(hash_id))
            fhash = self.cx.cu.execute("select hash from hashes where rowid=?",(hash_id,)).fetchone()
            print("fhash:",fhash)
            for path,name in self.cx.execute("select path,name from files where hash=?",(fhash,)):
                print("path,name:",path,name)
                p = elem("p")
                li += p
                a = elem("a")
                p += a
                a.attrt(("href","file:///"+path))
                a.txt(name)
        with open(ns.html_output,"w",encoding="utf-8") as htmlfile:
            htmlfile.write(str(root))
        os.startfile(ns.html_output)


db = Library()

print("db.cx:",db.cx)

if ns.scan:
    db.populate_files_from(ns.path,extensions=ns.ext)

if ns.analyze:
    db.analyze()

if ns.dump:
    list(map(print,db.cx.iterdump()))

if ns.dups:
    db.list_dups()

