SELECT 
    s.subject_name AS subject_,
    t.teacher_name,
    ROUND(AVG(CASE WHEN er.exam_event = 'Exam 1' THEN er.exam_score END), 2) AS average_exam_1_score,
    ROUND(AVG(CASE WHEN er.exam_event = 'Exam 2' THEN er.exam_score END), 2) AS average_exam_2_score
FROM 
    exam_result er
JOIN 
    subject s ON er.subject_id = s.subject_id
JOIN 
    teacher t ON s.subject_id = t.subject_id
GROUP BY 
    s.subject_name, t.teacher_name
ORDER BY 
    s.subject_name, t.teacher_name;
