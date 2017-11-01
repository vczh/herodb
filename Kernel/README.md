This is a draft.

# HIGH-LEVEL FEATURES
- Memory DB
- External resource accessing
  - Auto Backup to file group ring (maintain diff bitmaps for all file groups)
  - Generate the whole data base to C++
- Data Package
	- Package includes schema, data, procedures
	- Strong typed package non-circle dependency (package can access depended packages)
	- Multiple instances of one data package
	- Objects are allocated and stored inside one package, an object cannot be stored in a package that did not create it
	- Object can be deleted, but the handle is never reused
	- A data package can be deleted as a whole operation, which require all other packages that depend on it are deleted
	
# KEYWORDS
- PRIMITIVE-TYPE
- `enum`, `struct`, `class`
- `true`, `false`, `null`, `new`, `delete`
- `type`, `data`, `query`, `procedure`, `transaction`, `index`
- `package`, `using`, `public`,

# TYPE

### PRIMITIVE-TYPE
- `bool`
- `u?int(8|16|32|64)?`
- `float(32|64)?`
- `string`

### VALUE-TYPE
- PRIMITIVE-TYPE
- `enum {Spring, Summer, Autumn, Winter}`
- `struct {x : int, y : int}`

### COMPLEX-TYPE
- `A | B`: union type
- `[ TYPE ]`: enumerable type
- `VALUE-TYPE?`: nullable type

### CLASS-TYPE
- `class[:BASE-CLASS-TYPE]{x : int, y : int}`: reference counted class type, not nullable
- `CLASS-TYPE?`: nullable class type

### DATA-COLLECTION:

- Declaration:
```
type AttendExam = {
	s: Student;
	t: Teacher;
	e: Exam;
	score: int;
}

data AttendExams(AttendExam);
```
- Or Equivalently
```
data AttendExams(s: Student, t: Teacher, e: Exam, score: int);
```

- Index:
```
index AttendExams {
	Hash(s);
	Hash(e);
	Unique(e, s);
	partition(e) {
		Ordered(score);
		Unique(t);
	}
}
```

# QUERY

```
enum Gender {
	Male,
	Female
}

class Person {
	name : string,
	gender : Gender
}

data Father(father: Person, child: Person).
index Father {
	Unique(child);
}

data Mother(father: Person, child: Person).
index Mother {
	Unique(child);
}

data Relation(parent: Person, child: Person).
index Relation {
	partition(child) {
		Unique(Person);
	}
}
```

### Simple Query
```
query Parents(parent: Person, child: Child)
:-	Father(parent, child)
;	Mother(parent, child)
.

query GrandParents(grandParent: Person, grandChild: Person)
:-	(parent)
	:- Father(parent, child)
	;  Mother(parent, child)
	.,
	Parents(grandParent, parent)
.
```

### Output only argument
```
query Square(x: int) -> (x2: int)
:-	x2 <- x * x
.

query Solve(a: double, b: double, c: double) -> (x1: double, x2: double)
:-	delta <- b*b - 4*a*c,
	delta > 0,
	x1 <- (-b + delta) / (2 * a),
	x2 <- (-b - delta) / (2 * a)
;
```
- `<-` define the execution direction, it cannot run backward from x2 to x
- only out arguments can be put in the left side of `<-`
- Usage:
  - `x2 <- Square(x)`
  - `(x1, x2) <- Solve(a, b, c)`

### Cached Query
```
query GrandParents(grandParent: Person, grandChild: Person)
:-	Parents(grandParent, parent)
;	Parents(parent, grandChild)
.

index GrandParents {
	Hash(grandParent)
}
```
- When submit a query, the index for caching is used to see if the result has been calculated
  - Consider about:
    - Multiple index
    - Provided value covered by multiple index
    - Provide value not covered by any index
- If not, insert an index with the "calculating" status
- Adding an existing calculating index will cause an error (stop), which is not a failure (fail to pass a filter)
  - e.g. query is recursive on the same index value

### order_by, order_by_desc
```
data Exams(student: string, score: int).
index Exams {
	Unique(student);
}

query Top10() -> (student: string, score: int)
:-	Exams(student, score),
	order <- @order_by_desc(score),
	order < 10
;
```

### partition
```
data Exams(student: string, score: int).

query Top3ScorePerStudent(student: string) -> (score: int, order: int)
:-	Exams(student, score),
	@partition(student),
	order <- @order_by_desc(score),
	order < 3
;
```

### aggregation
```
data Exams(student: string, score: int).

// the order is not important
index AverageTop3ScorePerStudent {
	Unique(student);
}

query AverageTop3ScorePerStudent(student: string) -> (average: int)
:-	Exams(student, score),
	@partition(student),
	order <- @order_by_desc(score),
	order < 3,
	@aggregate(average(score)),
	average <- score
```

# UPDATE

### INSERT
```
data Exams(student: string, score: int).

query AddExam(student: string, score: int)
:-	@insert Exams(student, score)
;
```

### UPDATE
```
data Exams(student: string, score: int).

query UpdateExam(student: string, score: int)
:-	@update Exams(student, @score)
;
```

### REMOVE
```
data Exams(student: string, score: int).

query RemoveExam(student: string)
:-	@remove Exams(student, _)
;
```

# EXPRESSION (cannot run backward)
- Unary and Binary operators
- Name

# STATEMENT
- `QUERY`
- `OUTPUT <- EXPRESSION | QUERY`
- `[OUTPUT <- ]@COMMAND(EXPRESSION, ...)`
  - Each command has different requirements on expressions and return values
- `@COMMAND QUERY`

# GLOBAL READONLY VALUE
- `Zero <- 0.`: Constants
- `Students <- new StudentPackage.`: Package instance

# TRANSACTION
```
transaction query ...
```

# DATA PACKAGE

### DECLARATION
```
package NAME using P1, P2, P3 ...;

DECLARATIONS
public QUERY | TRANSACTION
```

### USING DATA PACKAGE
```
```
- A keyword refer to the current data package
- Instantiate a independent data package
- Discard a data package instance
- Instantiate a data package inheriting from another one
- Merge a inheriting data package to its parent

# ADMINISTRATION OPERATIONS

### INSTANTIATE A DATA PACKAGE
- Through protocol

### UPDATE SCHEMA
- Consider about:
  - Upload a new data package definition with data moving moving procedures

# SAMPLE (SCORE MANAGEMENT)

# SAMPLE (SYNTAX TREE)
